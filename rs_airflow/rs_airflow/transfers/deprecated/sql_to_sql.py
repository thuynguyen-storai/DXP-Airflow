import logging
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Any
from venv import logger

import pandas
from sqlalchemy.engine import Engine

from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context


class SqlQueryToSql(BaseOperator):
    """
    Deprecated: Use `rs_airflow.spark.transfers.sql_to_sql.SparkSqlToElasticsearch` instead.\

    This operator simply read all data from a SQL query and write to a SQL query

    Args:
        query (str): Executed query to fetch data (We currently only support direct query as string).
        source_sql_conn_id (str): Source SQL connection ID.\
            When using with providers, this could be Snowflake, SQL Server, etc.
        dest_sql_conn_id (str): Destination SQL connection ID.\
            When using with providers, this could be Snowflake, SQL Server, etc.
        dest_table_schema (str, optional): Specify the schema (if database flavor supports this).\
            If None, use default schema.
        dest_table_name (str): Name of SQL table.

        chunk_size (int): How many records to be processed per batch (default is 1000).
        executor_num (int): How many executor (Thread or Process) to be useed (default is 4).
        use_process_pool_over_thread_pool (bool): Whether to use ThreadPool or ProcessPool.\
            Experimentations show that it is best to keep using ThreadPool (which is the default value: False)
    """

    def __init__(
        self,
        query: str,
        source_sql_conn_id: str,
        dest_sql_conn_id: str,
        dest_table_schema: str | None,
        dest_table_name: str,
        chunk_size=1000,
        executor_num=4,
        use_process_pool_over_thread_pool=False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.query = query

        self.source_sql_conn_id = source_sql_conn_id

        self.dest_sql_conn_id = dest_sql_conn_id
        self.dest_table_schema = dest_table_schema
        self.dest_table_name = dest_table_name

        # Pandas related parameters
        self.chunk_size = chunk_size

        # Multi-processing to boost performance
        self.executor_num = executor_num
        self.use_process_pool_over_thread_pool = use_process_pool_over_thread_pool

    def execute(self, context: Context) -> Any:
        self.execute_single_thread()

    def execute_concurrently(self):
        source_sql_engine = self._get_sql_engine_from_conn(self.source_sql_conn_id)
        dest_sql_engine = self._get_sql_engine_from_conn(
            self.dest_sql_conn_id,
            {"execution_options": {"isolation_level": "AUTOCOMMIT", "pool_size": 0}},
        )

        logger = logging.getLogger(__name__)

        processing_batches = pandas.read_sql_query(
            self.query, con=source_sql_engine, chunksize=self.chunk_size
        )
        for batch in processing_batches:
            batch.to_sql(
                self.dest_table_name,
                con=dest_sql_engine,
                schema=self.dest_table_schema,
                if_exists="append",
                index=False,
                method="multi",
            )

            logger.debug("Processed one batch")

        # with executor_type(self.executor_num) as execution_pool:
        #     processing_batches = pandas.read_sql_query(self.query, con=source_sql_engine, chunksize=self.chunk_size)

        #     def _process_sql_batch():
        #         # processing_batches: Generator[pandas.DataFrame, None, None]
        #         while batch := next(processing_batches):
        #             batch.to_sql(
        #                 self.dest_table_name,
        #                 con=dest_sql_engine,
        #                 schema=self.dest_table_schema,
        #                 if_exists="append",
        #                 index=False,
        #                 method="multi",
        #             )

        #             logger.debug("Processed one batch")

        #     # force list finalized
        #     for _ in execution_pool.map(_process_sql_batch, processing_batches):
        #         pass

    def execute_single_thread(self):
        source_sql_engine = self._get_sql_engine_from_conn(self.source_sql_conn_id)
        dest_sql_engine = self._get_sql_engine_from_conn(self.dest_sql_conn_id)

        processing_batches = pandas.read_sql_query(
            self.query, con=source_sql_engine, chunksize=self.chunk_size
        )

        for batch in processing_batches:
            batch.to_sql(
                self.dest_table_name,
                con=dest_sql_engine,
                schema=self.dest_table_schema,
                if_exists="append",
                index=False,
                method="multi",
            )

            logger.debug("Processed one batch")

    def _get_sql_engine_from_conn(self, sql_conn_id: str, engine_kwargs=None) -> Engine:
        airflow_connection = BaseHook.get_connection(conn_id=sql_conn_id)
        try:
            return airflow_connection.get_hook().get_sqlalchemy_engine(engine_kwargs)
        except Exception as e:
            raise AirflowFailException(e)

    def _get_execution_pool_type(self):
        return (
            ProcessPoolExecutor
            if self.use_process_pool_over_thread_pool
            else ThreadPoolExecutor
        )
