import collections.abc
from datetime import datetime, timedelta
from typing import Any, Iterable, TypedDict

from airflow.models.abstractoperator import TaskStateChangeCallback
from airflow.models.baseoperator import TaskPostExecuteHook, TaskPreExecuteHook
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup


# Delay for future uses
class AirflowBaseOperatorArgument(TypedDict):
    task_id: str
    owner: str
    email: str | Iterable[str] | None
    email_on_retry: bool
    email_on_failure: bool
    retries: int | None
    retry_delay: timedelta | float
    retry_exponential_backoff: bool
    max_retry_delay: timedelta | float | None
    start_date: datetime | None
    end_date: datetime | None
    depends_on_past: bool
    ignore_first_depends_on_past: bool
    wait_for_past_depends_before_skipping: bool
    wait_for_downstream: bool
    dag: DAG | None
    params: collections.abc.MutableMapping | None
    default_args: dict | None
    priority_weight: int
    weight_rule: str
    queue: str
    pool: str | None
    pool_slots: int
    sla: timedelta | None
    execution_timeout: timedelta | None
    on_execute_callback: None | TaskStateChangeCallback | list[TaskStateChangeCallback]
    on_failure_callback: None | TaskStateChangeCallback | list[TaskStateChangeCallback]
    on_success_callback: None | TaskStateChangeCallback | list[TaskStateChangeCallback]
    on_retry_callback: None | TaskStateChangeCallback | list[TaskStateChangeCallback]
    pre_execute: TaskPreExecuteHook | None
    post_execute: TaskPostExecuteHook | None
    trigger_rule: str
    resources: dict[str, Any] | None
    run_as_user: str | None
    task_concurrency: int | None
    max_active_tis_per_dag: int | None
    max_active_tis_per_dagrun: int | None
    executor_config: dict | None
    do_xcom_push: bool
    inlets: Any | None
    outlets: Any | None
    task_group: TaskGroup | None
    doc: str | None
    doc_md: str | None
    doc_json: str | None
    doc_yaml: str | None
    doc_rst: str | None
