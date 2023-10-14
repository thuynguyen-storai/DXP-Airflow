import time
from typing import Tuple

import requests
from requests.auth import HTTPBasicAuth

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class StreamsetsHook(BaseHook):
    """
    A customizable StreamSets Hook

    References:

    Params:
        streamsets_conn_id: ID of connection
        polling_seconds: Number of seconds between pings / polling
    """

    # name of the parameter which receives the connection id
    conn_name_attr = "streamsets_conn_id"
    conn_type = "streamsets"

    hook_name = "Streamsets"

    def __init__(self, streamsets_conn_id: str, polling_seconds=10):
        super().__init__()

        self.streamsets_conn_id = streamsets_conn_id
        self.polling_seconds = polling_seconds

        self._conn: requests.Session | None = None

        self.base_url = self.get_base_url()

    def get_conn(self) -> requests.Session:
        if self._conn:
            return self._conn

        session = requests.Session()

        session.headers = {"X-Requested-By": "sdc"}

        connection = self.get_connection(self.streamsets_conn_id)
        decrypted_password = connection.get_password()
        if not decrypted_password:
            raise ValueError("decrypted_password is not set")
        session.auth = HTTPBasicAuth(connection.login, decrypted_password)

        self._conn = session
        return session

    def test_connection(self) -> Tuple[bool, str]:
        try:
            session = self.get_conn()
            response = session.get(
                f"{self.base_url}/rest/v1/system/info/currentUser",
            )
            assert response.ok
            return True, "Connection success"
        except Exception as e:
            return False, str(e)

    def get_base_url(self) -> str:
        connection = self.get_connection(self.streamsets_conn_id)
        return connection.host

    def start_pipeline(self, pipeline_id: str):
        self._verify_pipeline_initial_state(pipeline_id)
        self._trigger_start_pipeline(pipeline_id)

        while True:
            pipeline_is_finished = self._polling_pipeline_run_status(pipeline_id)
            if pipeline_is_finished:
                break

            time.sleep(self.polling_seconds)

    def _verify_pipeline_initial_state(self, pipeline_id: str):
        pipeline_status = self._get_pipeline_status(pipeline_id)

        if pipeline_status in StreamsetsState.initial_invalid_states:
            raise AirflowException("Pipeline in initial invalid states")

    def _trigger_start_pipeline(self, pipeline_id: str):
        connection = self.get_conn()
        response = connection.post(url=f"{self.base_url}/rest/v1/pipeline/{pipeline_id}/start")
        self._check_response(response)

    def _polling_pipeline_run_status(self, pipeline_id: str) -> bool:
        """
        Polling StreamSets to check for pipeline run status

        Args:
            pipeline_id (str):

        Raises:
            AirflowException: pipeline failed

        Returns:
            bool: whether pipeline is finished
        """
        pipeline_status = self._get_pipeline_status(pipeline_id)

        if pipeline_status in StreamsetsState.terminate_states:
            return True

        if pipeline_status in StreamsetsState.running_states:
            return False

        raise AirflowException("Pipeline failed")

    def _get_pipeline_status(self, pipeline_id: str) -> str:
        """
        Get status of pipeline

        Args:
            pipeline_id (str):

        Raises:
            AirflowException: Any failed attempt to send REST API request

        Returns:
            str: pipeline status
        """
        connection = self.get_conn()
        response = connection.request(method="GET", url=f"{self.base_url}/rest/v1/pipeline/{pipeline_id}/status")
        self._check_response(response)

        try:
            pipeline_status: str = response.json()["status"]
            self.log.debug("Received pipeline id: %s - status: %s", pipeline_id, pipeline_status)
            return pipeline_status
        except requests.JSONDecodeError:
            raise AirflowException("Failed to parsed response error")

    def _check_response(self, response: requests.Response) -> None:
        """Check the status code and raise on failure.

        :param response: A requests response object.
        :raise AirflowException: If the response contains a status code not
            in the 2xx and 3xx range.
        """
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            self.log.error("HTTP error: %s", response.reason)
            self.log.error(response.text)
            raise AirflowException(str(response.status_code) + ":" + response.reason)


class StreamsetsState:
    # If in these, we should not start pipeline
    initial_invalid_states = [
        "STARTING",
        "STARTING_ERROR",
        "DELETED",
        "RUNNING",
        "FINISHING",
        "RETRY",
        "STOPPING",
        "STOPPING_ERROR",
        "DELETED",
    ]

    # Meet these states means our pipeline is surely failed
    failed_states = ["START_ERROR", "RUN_ERROR", "STOP_ERROR", "CONNECT_ERROR"]

    # Pipeline is still running
    running_states = [
        "STARTING",
        "RUNNING",
        "STARTING_ERROR",
        "RUNNING",
        "RUNNING_ERROR",
        "FINISHING",
        "RETRY",
        "STOPPING",
        "STOPPING_ERROR",
    ]

    # Pipeline is finished, unclear if it was success or fail
    terminate_states = [
        "FINISHED",
        "STOPPED",
        "KILLED",
        "DELETED",
        "DISCONNECTING",
        "DISCONNECTED",
        "CONNECTING",
        "CONNECT_ERROR",
    ]

    all_states = [
        "EDITED",
        "STARTING",
        "STARTING_ERROR",
        "START_ERROR",
        "RUNNING",
        "RUNNING_ERROR",
        "RUN_ERROR",
        "FINISHING",
        "FINISHED",
        "RETRY",
        "KILLED",
        "STOPPING",
        "STOPPED",
        "STOPPING_ERROR",
        "STOP_ERROR",
        "DISCONNECTING",
        "DISCONNECTED",
        "CONNECTING",
        "CONNECT_ERROR",
        "DELETED",
    ]
