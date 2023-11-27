"""
This is a WIP
"""

import logging
import time
from datetime import timedelta
from typing import Any

from airflow.hooks.base import BaseHook
from airflow.configuration import conf
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.temporal import TimeDeltaTrigger
from airflow.utils.context import Context
from airflow.exceptions import ParamValidationError

from ..hooks import StreamsetsHook

# Python 3.11 types
# from typing import TypeVarTuple


class TriggerStreamsetsSensor(BaseSensorOperator):
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
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.streamsets_conn_id = streamsets_conn_id
        self.hook: StreamsetsHook = BaseHook.get_hook(streamsets_conn_id)  # type: ignore
        if not isinstance(self.hook, StreamsetsHook):
            raise ParamValidationError(
                "Connection ID must belong to StreamSets: %s", streamsets_conn_id
            )

        self.pipeline_id = pipeline_id
        self.polling_period = polling_period

        self.deferrable = deferrable

    def execute(self, context: Context) -> None:
        self.hook.start_pipeline(self.pipeline_id)

        self.recheck_streamsets(context)

    def recheck_streamsets(
        self,
        context: Context,
        event: dict[str, Any] | None = None,
    ) -> None:
        logger = logging.getLogger(__name__)

        logger.info("Rechecking StreamSets")
        logger.info("deferrable: %s", self.deferrable)
        # We have no more work to do here. Mark as complete.
        pipeline_is_finished = self.hook.polling_pipeline_run_status(self.pipeline_id)
        if pipeline_is_finished:
            return

        logger.info("Defer execution: StreamSets")
        if self.deferrable:
            logger.info("Defer execution: StreamSets")

            self.defer(
                trigger=TimeDeltaTrigger(self.polling_period),
                method_name="recheck_streamsets",
            )
        else:
            time.sleep(self.polling_period.total_seconds())
