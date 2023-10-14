import datetime
import logging
from multiprocessing import Pool
from time import perf_counter
from typing import Any
from venv import logger

import elasticsearch.helpers
import pandas
from elasticsearch.serializer import JSONSerializer
from sqlalchemy import text
from sqlalchemy.engine import Engine

from airflow.exceptions import AirflowFailException
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
        batch_size (int): How many records to be processed per batch (default is 1000)
        num_of_process (int): As this use a multiprocess.Pool at its core, how many process should be allocated to this (default is 4)
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
        batch_size=1000,
        num_of_process=4,
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

        self.batch_size = batch_size

    def execute(self, context: Context) -> Any:
        self.use_pandas()

    def use_pandas(self, process_num=4):
        snowflake_sql_engine: Engine = self.snowflake_hook.get_sqlalchemy_engine()

        start_time = perf_counter()
        with snowflake_sql_engine.connect() as snowflake_connection, Pool(process_num) as process_pool:
            batches = pandas.read_sql_table(
                table_name=self.table_name,
                schema=self.table_schema,
                con=snowflake_connection,
                chunksize=self.batch_size,
            )

            # force list finalized
            for _ in process_pool.imap_unordered(self._process_sql_batch, batches):
                pass

        finish_time = perf_counter()
        logger.info("execution time: %d", finish_time - start_time)

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
