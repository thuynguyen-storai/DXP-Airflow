import logging
from typing import Any

from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator
from rs_airflow.spark.libs.jdbc_config_manager import JdbcConfigManager
from rs_airflow.spark.libs.spark_builder import SparkBuilder


class SparkSqlToSql(BaseOperator):
    def __init__(
        self,
        source_conn_id: str,
        source_query: str,
        dest_conn_id: str,
        dest_table_name: str,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.source_conn_id = source_conn_id
        self.source_query = source_query

        self.dest_conn_id = dest_conn_id
        self.dest_table_name = dest_table_name

        self._logger = logging.getLogger(__name__)

    def execute(self, context) -> Any:
        self._logger.info("Executing operator SparkSqlToSql")

        source_connection = BaseHook.get_connection(self.source_conn_id)
        source_jdbc_configs = JdbcConfigManager.generate_jdbc_configs_for_sqlalchemy(source_connection)

        spark = SparkBuilder.generate_spark_session()
        df = spark.read.format("jdbc").options(**source_jdbc_configs).option("query", self.source_query).load()

        dest_connection = BaseHook.get_connection(self.dest_conn_id)
        dest_jdbc_configs = JdbcConfigManager.generate_jdbc_configs_for_sqlalchemy(dest_connection)
        df_writer = df.write.format("jdbc").options(**dest_jdbc_configs).option("dbtable", self.dest_table_name)
        df_writer.save(mode="overwrite")

        self._logger.info("Finished operator SparkSqlToSql")
