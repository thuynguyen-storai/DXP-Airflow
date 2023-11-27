from datetime import timedelta
import time
from typing import Any

from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context

from ..hooks import StreamsetsHook

# Python 3.11 types
# from typing import TypeVarTuple


class TriggerStreamsetsOperator(BaseOperator):
    """
    StreamSets Operator - for invoking StreamSet's pipelines, monitoring runs, etc, ...

    Parameters:
        streamsets_conn_id (str): A (Generic possible) connection to StreamSets
        pipeline_id (str): ID of running pipeline

    """

    def __init__(
        self,
        streamsets_conn_id: str,
        pipeline_id: str,
        polling_period=timedelta(seconds=10),
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        if not streamsets_conn_id:
            raise ValueError("streamsets_conn_id is not set")
        self.streamsets_conn_id = streamsets_conn_id

        if not pipeline_id:
            raise ValueError("pipeline_id is not set")
        self.pipeline_id = pipeline_id

        self.polling_period = polling_period

    def execute(self, context: Context) -> Any:
        hook = StreamsetsHook(self.streamsets_conn_id)
        hook.start_pipeline(self.pipeline_id)

        while True:
            pipeline_is_finished = hook.polling_pipeline_run_status(self.pipeline_id)
            if pipeline_is_finished:
                break

            time.sleep(self.polling_period.total_seconds())

    def on_kill(self) -> None:
        return super().on_kill()
