from datetime import datetime

from rs_airflow.transfers.sql_to_elasticsearch import SqlTableToElasticOperator
from airflow.decorators import dag


@dag("snowflake_to_elasticsearch_dag", start_date=datetime(2023, 10, 3), schedule=None)
def snowflake_to_elasticsearch_dag():
    # Record count = 659,380
    SqlTableToElasticOperator(
        table_schema="PUBLIC",
        table_name="PRODUCT",
        # table_name="OfferStagingRuning",
        id_column="Id",
        sql_conn_id="snowflake_conn",
        elastic_conn_id="elasticsearch_conn",
        elastic_index_name="test_airflow",
        task_id="testing",
    )


snowflake_to_elasticsearch_dag()

if __name__ == "__main__":
    snowflake_to_elasticsearch_dag().test()
