import logging
import multiprocessing
from typing import Any

from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator
from rs_airflow.spark.libs.jdbc_config_manager import JdbcConfigManager
from rs_airflow.spark.libs.spark_builder import SparkBuilder


class SparkSqlToElasticsearch(BaseOperator):
    def __init__(
        self,
        source_conn_id: str,
        source_query: str,
        id_column: str,
        dest_es_conn_id: str,
        dest_index_name: str,
        overwrite_es_index=False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.source_conn_id = source_conn_id
        self.source_query = source_query
        self.id_column = id_column

        self.dest_es_conn_id = dest_es_conn_id
        self.dest_index_name = dest_index_name

        self._logger = logging.getLogger(__name__)

    def execute(self, context) -> Any:
        spark = SparkBuilder.generate_spark_session()

        source_connection = BaseHook.get_connection(conn_id=self.source_conn_id)
        source_jdbc_configs = JdbcConfigManager.generate_jdbc_configs_for_sqlalchemy(
            source_connection
        )

        spark_executor_cores = max(multiprocessing.cpu_count() - 2, 1)

        input_df = (
            spark.read.format("jdbc")
            .options(**source_jdbc_configs)
            .option(
                "numPartitions", str(spark_executor_cores)
            )  # No effect, testing purpose only
            .option("query", self.source_query)
            .load()
        )

        es_host_configs = self._get_conf_for_es_host()

        (
            input_df.write.format("org.elasticsearch.spark.sql")
            .options(**es_host_configs)
            .option("es.mapping.id", self.id_column)
            .mode("append")
            .save(self.dest_index_name)
        )

        self._logger.info("Finished operator SparkSqlToElasticsearch")

    def _get_conf_for_es_host(self) -> dict[str, str]:
        elastic_conn = BaseHook.get_connection(self.dest_es_conn_id)

        return {
            "es.index.auto.create": "true",
            "es.nodes": elastic_conn.host,
            "es.port": elastic_conn.port,
        }
