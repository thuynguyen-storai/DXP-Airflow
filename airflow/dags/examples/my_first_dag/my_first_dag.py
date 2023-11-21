import datetime

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


@dag(schedule=None, start_date=datetime.datetime(2023, 9, 27), tags=["offer", "example"])
def my_first_dag():
    @task.kubernetes(image="python:3.8-slim-buster")
    def delay_10_secs():
        import time

        time.sleep(10)

    python_delay = BashOperator(task_id="bash_task", bash_command="sleep 10 && uname -a")

    empty_op = EmptyOperator(task_id="do_nothing")
    empty_op >> python_delay
    empty_op >> delay_10_secs()


my_first_dag()

if __name__ == "__main__":
    my_first_dag().test()
