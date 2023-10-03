import datetime
import json

from airflow.decorators import dag, task
from airflow.providers.http.operators.http import SimpleHttpOperator


@dag(schedule=None, start_date=datetime.datetime(2023, 9, 27))
def my_first_dag():
    send_webhook = SimpleHttpOperator(
        task_id="send_http",
        http_conn_id="webhook",
        endpoint="26ef0e51-594a-44e3-bfad-fe0a2345eced",
        method="post",
        data=json.dumps({"priority": 5}),
        headers={"Content-Type": "application/json"},
        log_response=True,
    )

    @task
    def send_http():
        return 1

    send_webhook >> send_http()


my_first_dag()

if __name__ == "__main__":
    my_first_dag().test()
