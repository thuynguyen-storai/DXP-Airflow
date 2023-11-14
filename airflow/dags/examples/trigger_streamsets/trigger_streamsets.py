import datetime

from airflow.decorators import dag
from rs_airflow.streamsets import TriggerStreamsetsOperator

__version__ = "0.0.1"


@dag("trigger_streamsets", schedule=None, start_date=datetime.datetime(2023, 9, 29))
def trigger_streamsets():
    TriggerStreamsetsOperator(
        task_id="trigger_streamsets_operator",
        streamsets_conn_id="streamsets_url",
        pipeline_id="Experimen187e5e9f-2cf6-4b29-807b-360302a0ba28",
    )


trigger_streamsets()


if __name__ == "__main__":
    trigger_streamsets().test()
