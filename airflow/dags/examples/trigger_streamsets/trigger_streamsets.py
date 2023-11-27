from datetime import datetime, timedelta

from airflow import DAG
from rs_airflow.streamsets.operators import TriggerStreamsetsOperator

__version__ = "0.0.1"


with DAG(dag_id="trigger_streamsets", schedule=None, start_date=datetime(2023, 9, 29)) as trigger_streamsets:
    TriggerStreamsetsOperator(
        task_id="trigger_streamsets_operator",
        streamsets_conn_id="streamsets_url",
        pipeline_id="Experimen187e5e9f-2cf6-4b29-807b-360302a0ba28",
        polling_period=timedelta(seconds=5),
    )


if __name__ == "__main__":
    trigger_streamsets.test()
