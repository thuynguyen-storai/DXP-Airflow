import datetime
import logging

from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

connection_args = {
    "conn_id": "airflow_db",
    "conn_type": "Postgres",
    "host": "postgres",
    "schema": "postgres",
    "login": "postgres",
    "password": "postgres",
    "port": 5432,
}

logger = logging.getLogger(__name__)


@dag(schedule=None, start_date=datetime.datetime(2023, 9, 27))
def my_second_dags():
    SQLExecuteQueryOperator(
        task_id="fetch_data",
        sql="SELECT * FROM customers",
        conn_id="postgres_shopping",
        # database='shopping',
        show_return_value_in_logs=True,
    )


my_second_dags()

if __name__ == "__main__":
    my_second_dags().test()
