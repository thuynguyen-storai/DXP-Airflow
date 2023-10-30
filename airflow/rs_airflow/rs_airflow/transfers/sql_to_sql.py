import datetime
import functools
import logging
from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import Pool
from time import perf_counter
from typing import Any, Literal, TypeAlias
from venv import logger

import elasticsearch.helpers
import numba
import pandas
from elasticsearch.serializer import JSONSerializer
from sqlalchemy.engine import Engine, Connection

from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook
from airflow.utils.context import Context
from airflow.exceptions import AirflowFailException


class SqlQueryToSql(BaseOperator):
    """
    This operator simply read all data from a SQL query and write to a SQL query

    Args:
        query (str): Executed query to fetch data (We currently only support direct query as string).
        source_sql_conn_id (str): Source SQL connection ID .When using with providers, this could be Snowflake, SQL Server, etc.
        dest_sql_conn_id (str): Destination SQL connection ID. When using with providers, this could be Snowflake, SQL Server, etc.
        dest_table_schema (str, optional): Specify the schema (if database flavor supports this). If None, use default schema.
        dest_table_name (str): Name of SQL table.

        chunk_size (int): How many records to be processed per batch (default is 1000).
        executor_num (int): How many executor (Thread or Process) to be useed (default is 4).
        use_process_pool_over_thread_pool (bool): Whether to use ThreadPool or ProcessPool. Experimentations show that it is best to keep using ThreadPool (which is the default value: False)
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
        source_sql_engine = self._get_sql_engine_from_conn(self.source_sql_conn_id)
        dest_sql_engine = self._get_sql_engine_from_conn(self.dest_sql_conn_id)

        executor_type = self._get_execution_pool_type()

        with (
            source_sql_engine.connect() as source_connection,
            dest_sql_engine.connect() as dest_connection,
            executor_type(self.executor_num) as execution_pool,
        ):
            processing_batches = pandas.read_sql_query(self.query, con=source_connection, chunksize=self.chunk_size)

            def _process_sql_batch(batch: pandas.DataFrame):
                batch.to_sql(
                    self.dest_table_name,
                    con=dest_connection,
                    schema=self.dest_table_schema,
                    if_exists="append",
                    index=False,
                )

            # force list finalized
            for _ in execution_pool.map(_process_sql_batch, processing_batches):
                pass

    def _get_sql_engine_from_conn(self, sql_conn_id: str) -> Engine:
        airflow_connection = BaseHook.get_connection(conn_id=sql_conn_id)
        try:
            return airflow_connection.get_hook().get_sqlalchemy_engine()
        except Exception as e:
            raise AirflowFailException(e)

    def _get_execution_pool_type(self):
        return ProcessPoolExecutor if self.use_process_pool_over_thread_pool else ThreadPoolExecutor
