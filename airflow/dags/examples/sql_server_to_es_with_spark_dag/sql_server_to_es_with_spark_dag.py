from datetime import datetime

import kubernetes.client as k8s

from airflow.decorators import dag
from rs_airflow.spark.transfers import SparkSqlToElasticsearch


@dag(dag_id="sql_server_to_es_with_spark_dag", start_date=datetime(2023, 11, 9), schedule=None)
def sql_server_to_es_with_spark_dag():
    SparkSqlToElasticsearch(
        task_id="sql_to_es_spark",
        source_conn_id="sqlserver_conn",
        source_query="select * from Product",
        id_column="Id",
        dest_es_conn_id="elasticsearch_conn",
        dest_index_name="test_airflows",
        executor_config={
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            resources=k8s.V1ResourceRequirements(
                                requests={"cpu": "2000m", "memory": "2048Mi"}, limits={"memory": "2048Mi"}
                            ),
                        )
                    ]
                )
            )
        },
    )


sql_server_to_es_with_spark_dag()

if __name__ == "__main__":
    sql_server_to_es_with_spark_dag().test()
