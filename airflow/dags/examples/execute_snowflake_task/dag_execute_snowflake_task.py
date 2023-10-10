import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from airflow.decorators import dag

__version__ = "0.0.1"


@dag("dag_execute_snowflake_task", schedule=None, start_date=datetime.datetime(2023, 9, 29))
def dag_execute_snowflake_task():
    run_task = SQLExecuteQueryOperator(
        task_id="execute_snowflake_task",
        sql="./snowflake_task.sql",
        conn_id="snowflake_conn",
        autocommit=True,
        split_statements=False,
    )


dag_execute_snowflake_task()


if __name__ == "__main__":
    dag_execute_snowflake_task().test()
