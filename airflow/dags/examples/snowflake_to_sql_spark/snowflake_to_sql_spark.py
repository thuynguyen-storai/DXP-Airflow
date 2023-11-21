from datetime import datetime

from airflow.decorators import dag
from rs_airflow.spark.transfers.sql_to_sql import SparkSqlToSql


@dag("snowflake_to_sql_spark_dag", start_date=datetime(2023, 11, 6), schedule=None)
def snowflake_to_sql_spark_dag():
    SparkSqlToSql(
        task_id="sql_to_sql",
        source_conn_id="snowflake_conn",
        source_query="select * from rs_schema.product",
        dest_conn_id="sql_staging_conn",
        dest_table_name="test_airflow_products",
    )


snowflake_to_sql_spark_dag()

if __name__ == "__main__":
    snowflake_to_sql_spark_dag().test()
