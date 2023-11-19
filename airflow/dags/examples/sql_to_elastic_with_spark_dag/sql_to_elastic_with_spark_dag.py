from datetime import datetime

from airflow.decorators import dag
from rs_airflow.spark.transfers import SparkSqlToElasticsearch


@dag(dag_id="sql_to_elastic_with_spark_dag", start_date=datetime(2023, 11, 9), schedule=None)
def sql_to_elastic_with_spark_dag():
    SparkSqlToElasticsearch(
        task_id="sql_to_es_spark",
        source_conn_id="snowflake_conn",
        source_query="select * from RS_SCHEMA.PRODUCT",
        id_column="ID",
        dest_es_conn_id="elasticsearch_conn",
        dest_index_name="test_airflows",
    )


sql_to_elastic_with_spark_dag()

if __name__ == "__main__":
    sql_to_elastic_with_spark_dag().test()
