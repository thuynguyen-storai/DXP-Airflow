from datetime import datetime

from airflow.decorators import dag

from rs_airflow.transfers.deprecated import SqlQueryToSql
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


@dag("snowflake_to_sql_server_dag", start_date=datetime(2023, 11, 2), schedule=None)
def snowflake_to_sql_server_dag():
    truncate_sql = SQLExecuteQueryOperator(
        task_id="truncate_sql_table",
        sql="IF OBJECT_ID('test_airflow_product', 'U') IS NOT NULL TRUNCATE TABLE test_airflow_product;",
        conn_id="sql_staging_conn",
        split_statements=False,
    )

    product_snowflake_to_sql = SqlQueryToSql(
        task_id="SqlQueryToSql",
        query="select * from rs_schema.product",
        source_sql_conn_id="snowflake_conn",
        dest_sql_conn_id="sql_staging_conn",
        dest_table_schema="dbo",
        dest_table_name="test_airflow_product",
    )

    truncate_sql >> product_snowflake_to_sql

    # @task.kubernetes(image="python:3.8-slim-buster")


snowflake_to_sql_server_dag()

if __name__ == "__main__":
    snowflake_to_sql_server_dag().test()
