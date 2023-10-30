import datetime
import logging
from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import Pool
from time import perf_counter
from typing import Any
from venv import logger

import elasticsearch.helpers
import pandas
from elasticsearch.serializer import JSONSerializer
from sqlalchemy.engine import Engine

from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook
from airflow.utils.context import Context


class SqlTableToElasticOperator(BaseOperator):
    """
    This operator simply read all data from a SQL Table and INDEX it to Elasticsearch

    Args:
        table_schema (str): Schema of SQL Table (e.g. `dbo`, `public`)
        table_name (str): SQL table name
        id_column (str): Column name that would be used as ID for Elasticsearch document
        sql_conn_id (str): Connection ID of SQL Table. When using with providers, this could be Snowflake, SQL Server, etc.
        elastic_conn_id (str): Connection ID of Elasticsearch
        elastic_index_name (str): Name of the destination index

        chunk_size (int): How many records to be processed per batch (default is 1000).
        executor_num (int): How many executor (Thread or Process) to be useed (default is 4).
        use_process_pool_over_thread_pool (bool): Whether to use ThreadPool or ProcessPool. Experimentations show that it is best to keep using ThreadPool (which is the default value: False)
    """

    ui_color = "#f0f0e4"

    def __init__(
        self,
        table_schema: str,
        table_name: str,
        id_column: str,
        sql_conn_id: str,
        elastic_conn_id: str,
        elastic_index_name: str,
        chunk_size=1000,
        executor_num=4,
        use_process_pool_over_thread_pool=False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.table_schema = table_schema
        self.table_name = table_name
        self.id_column = id_column

        self._logger = logging.getLogger(__name__)

        snowflake_connection = BaseHook.get_connection(conn_id=sql_conn_id)
        self.snowflake_hook = snowflake_connection.get_hook()

        elastic_urls = self._get_elastic_urls(elastic_conn_id)
        self.elastic_hook = ElasticsearchPythonHook(elastic_urls, {"serializer": CustomSerializer()})

        self.elastic_index_name = elastic_index_name

        self.chunk_size = chunk_size
        self.executor_num = executor_num
        self.use_process_pool_over_thread_pool = use_process_pool_over_thread_pool

    def execute(self, context: Context) -> Any:
        self.use_pandas(self.executor_num)

    def use_pandas(self, process_num=4):
        snowflake_sql_engine: Engine = self.snowflake_hook.get_sqlalchemy_engine()

        pool_type: type[Executor] = (
            ProcessPoolExecutor if self.use_process_pool_over_thread_pool else ThreadPoolExecutor
        )

        start_time = perf_counter()
        with snowflake_sql_engine.connect() as snowflake_connection, pool_type(process_num) as process_pool:
            batches = pandas.read_sql_table(
                table_name=self.table_name,
                schema=self.table_schema,
                con=snowflake_connection,
                chunksize=self.chunk_size,
            )

            # force list finalized
            for _ in process_pool.map(self._process_sql_batch, batches):
                pass

        finish_time = perf_counter()
        logger.debug("execution time: %d", finish_time - start_time)

    def _process_sql_batch(self, batch: pandas.DataFrame):
        elastic_client = self.elastic_hook.get_conn
        action_list = self._convert_pandas_df_to_elastic_action_list(batch)

        elasticsearch.helpers.bulk(
            elastic_client,
            actions=action_list,
            index=self.elastic_index_name,
        )

    def _convert_pandas_df_to_elastic_action_list(self, input: pandas.DataFrame) -> Any:
        input["doc"] = input.to_dict(orient="records")
        input["_id"] = input[self.id_column]
        return input[["_id", "doc"]].to_dict(orient="records")

    def _get_elastic_urls(self, conn_id: str):
        elastic_connection = BaseHook.get_connection(conn_id=conn_id)

        host = elastic_connection.host
        port = int(elastic_connection.port) or 9200

        # user=str(elastic_connection.login) or None,
        # password=elastic_connection.password or None,

        scheme = str(elastic_connection.schema) or "http"

        elastic_url = f"{scheme}://{host}:{port}"

        return [elastic_url]


class CustomSerializer(JSONSerializer):
    def default(self, obj):
        if isinstance(obj, datetime.datetime) or isinstance(obj, datetime.time):
            return obj.isoformat()
        return JSONSerializer.default(self, obj)
