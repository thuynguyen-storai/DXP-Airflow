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

    def __init__(self, streamsets_conn_id: str, pipeline_id: str, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if not streamsets_conn_id:
            raise ValueError("streamsets_conn_id is not set")
        self.streamsets_conn_id = streamsets_conn_id

        if not pipeline_id:
            raise ValueError("pipeline_id is not set")
        self.pipeline_id = pipeline_id

    def execute(self, context: Context) -> Any:
        hook = StreamsetsHook(self.streamsets_conn_id)
        result = hook.start_pipeline(self.pipeline_id)
        return result

    def on_kill(self) -> None:
        return super().on_kill()
