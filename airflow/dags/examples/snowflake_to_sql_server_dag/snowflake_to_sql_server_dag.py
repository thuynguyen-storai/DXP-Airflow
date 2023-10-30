from datetime import datetime

from airflow.decorators import dag

from rs_airflow.transfers.sql_to_sql import SqlQueryToSql
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


@dag("azure_blob_to_sql_server_dag", start_date=datetime(2023, 10, 28), schedule=None)
def snowflake_to_sql_server_dag():
    truncate_sql = SQLExecuteQueryOperator(
        task_id="truncate_sql_table",
        sql="IF OBJECT_ID('test_airflow_product', 'U') IS NOT NULL DROP TABLE test_airflow_product",
        conn_id="sql_staging_conn",
        autocommit=True,
    )

    product_snowflake_to_sql = SqlQueryToSql(
        task_id="SqlQueryToSql",
        query="select * from public.product where SECONDARYPRODUCTIDENTIFIER",
        source_sql_conn_id="snowflake_conn",
        dest_sql_conn_id="sql_staging_conn",
        dest_table_schema="dbo",
        dest_table_name="test_airflow_product",
    )

    truncate_sql >> product_snowflake_to_sql


snowflake_to_sql_server_dag()

if __name__ == "__main__":
    snowflake_to_sql_server_dag().test()
