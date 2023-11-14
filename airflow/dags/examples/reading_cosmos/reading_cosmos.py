import datetime

from airflow.decorators import dag
from rs_airflow.cosmos.read_first_operator import ReadingCosmos

__version__ = "0.0.1"


@dag("reading_cosmos", schedule=None, start_date=datetime.datetime(2023, 9, 29))
def reading_cosmos():
    ReadingCosmos(task_id="reading_cosmos", azure_cosmos_conn_id="cosmos_conn")


reading_cosmos()


if __name__ == "__main__":
    reading_cosmos().test()
