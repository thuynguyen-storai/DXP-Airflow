import datetime
from typing import Any
from urllib.parse import urlparse

from azure.storage.blob import ContainerSasPermissions, generate_container_sas
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.context import Context


class AzureBlobToSnowflake(BaseOperator):
    """
    Using Snowflake Stage and `COPY INTO`, copy data from Azure Blob Storage to Snowflake table

    Parameters:
        source_blob_conn_id (str): Source Azure Blob storage connection ID.
        source_blob_container (str): Source Container name.
        source_blob_name (str): Source Blob name (including the path).
        dest_snowflake_conn_id (str): Destination Snowflake connection ID.
        dest_schema (str): Destination table schema.
        dest_table (str): Destination table name.
        dest_prerun_query (str): Script to run before `COPY INTO`
        input_file_format (str): Supporting file formats include CSV, JSON, Avro, ...
            See more: [Snowflake - Create Stage - Optional Parameters]\
            (https://docs.snowflake.com/en/sql-reference/sql/create-stage#optional-parameters)
        input_file_config (dict[str, v]): If exists, each key-value will be added to Snowflake Stage config.
            See more: [Snowflake - Create Stage - Format Type Options]\
            (https://docs.snowflake.com/en/sql-reference/sql/create-stage#format-type-options-formattypeoptions)
        copy_into_config (dict[str, Any]): Config to put into `COPY INTO` statement.

    Examples:

        ```python
        AzureBlobToSnowflake(
            task_id="blob_to_snowflake",
            source_blob_conn_id="sandbox_united_blob_conn",
            source_blob_container="dev-etl",
            source_blob_name="CampaignDIM_20230914100009.txt",
            dest_snowflake_conn_id="snowflake_conn",
            dest_schema="public",
            dest_table="test_airflow_copy_into",
            dest_prerun_query="TRUNCATE TABLE test_airflow_copy_into",
            input_file_config={"FIELD_DELIMITER": "|"},
        )
        ```
    """

    def __init__(
        self,
        source_blob_conn_id: str,
        source_blob_container: str,
        source_blob_name: str,
        dest_snowflake_conn_id: str,
        dest_schema: str,
        dest_table: str,
        dest_prerun_query: str,
        input_file_format: str = "csv",
        input_file_config: dict[str, Any] | None = None,
        copy_into_config: dict[str, Any] | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.source_blob_conn_id = source_blob_conn_id
        self.source_blob_container = source_blob_container
        self.source_blob_name = source_blob_name

        if not input_file_format:
            raise AirflowFailException("File format must be specified")
        self.input_file_format = input_file_format
        self.input_file_config = input_file_config
        self.copy_into_config = copy_into_config

        self.dest_snowflake_conn_id = dest_snowflake_conn_id
        self.dest_schema = dest_schema
        self.dest_table = dest_table
        self.dest_prerun_query = dest_prerun_query

    def execute(self, context: Context) -> Any:
        blob_url, blob_sas = self._generate_blob_sas()

        stage_name = f"{self.dest_schema}.stage_{context.get('task_instance_key_str')}"

        snowflake_engine = self._get_snowflake_engine()
        with snowflake_engine.engine.begin() as snowflake_conn:
            self._create_snowflake_stage(snowflake_conn, stage_name, blob_url, blob_sas)
            self._perform_copy_into(snowflake_conn, stage_name)

    def _generate_blob_sas(self) -> tuple[str, str]:
        input_blob_hook = WasbHook(self.source_blob_conn_id)
        container_client = input_blob_hook.blob_service_client.get_container_client(
            container=self.source_blob_container
        )

        blob_url = urlparse(container_client.url)
        azure_url = f"azure://{blob_url.netloc}{blob_url.path}"

        start_time = datetime.datetime.now(datetime.timezone.utc)
        expiry_time = start_time + datetime.timedelta(
            days=1  # Compensate for UTC mismatch
        )

        sas_token = generate_container_sas(
            account_name=str(container_client.account_name),
            container_name=self.source_blob_container,
            account_key=input_blob_hook.blob_service_client.credential.account_key,
            permission=ContainerSasPermissions(read=True, list=True),
            expiry=expiry_time,
            start=start_time,
        )

        return azure_url, sas_token

    def _create_snowflake_stage(
        self,
        snowflake_conn: Connection,
        stage_name: str,
        blob_url: str,
        sas_token: str,
    ):
        sql_file_config = self._parse_configs_for_sql_statement(self.input_file_config)
        create_stage_query = text(
            f"CREATE TEMPORARY STAGE {stage_name} \
                URL=:blob_url \
                CREDENTIALS=( AZURE_SAS_TOKEN=:sas_token ) \
                FILE_FORMAT=( TYPE=:file_type {sql_file_config})",
        )

        snowflake_conn.execute(
            create_stage_query,
            {
                "blob_url": blob_url,
                "sas_token": sas_token,
                "file_type": self.input_file_format,
            },
        )

    def _perform_prerun_query(self, snowflake_conn: Connection):
        if self.dest_prerun_query:
            snowflake_conn.execute(self._perform_prerun_query)

    def _perform_copy_into(self, snowflake_conn: Connection, stage_name: str):
        """
        Note: to quickly create table for `COPY INTO`

        ```sql
        CREATE TABLE test_airflow_copy_into
        USING TEMPLATE (
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
            FROM TABLE(
                INFER_SCHEMA(
                    LOCATION=>'@public.stage_azure_blob_to_snowflake_dag__blob_to_snowflake__20231115',
                    FILE_FORMAT => 'stage_azure_blob_to_snowflake_dag__blob_to_snowflake__20231115_file_format'
                )
            )
        );
        ```
        """
        table_full_name = f"{self.dest_schema}.{self.dest_table}"

        sql_copy_into_config = self._parse_configs_for_sql_statement(
            self.copy_into_config
        )

        snowflake_conn.execute(
            text(
                f"COPY INTO {table_full_name} FROM @{stage_name} {sql_copy_into_config} PATTERN=:file_pattern;"
            ),
            {"file_pattern": self.source_blob_name},
        )

    @staticmethod
    def _parse_configs_for_sql_statement(config: dict[str, str] | None = None) -> str:
        sql_config = ""
        if config:
            file_config_pair = [f"{key}={repr(value)}" for key, value in config.items()]
            sql_config = " ".join(file_config_pair)

        return sql_config

    def _get_snowflake_engine(self, engine_kwargs=None) -> Engine:
        dest_snowflake_hook = BaseHook.get_hook(self.dest_snowflake_conn_id)
        if not isinstance(dest_snowflake_hook, SnowflakeHook):
            raise AirflowFailException("Destination must be Snowflake")

        return dest_snowflake_hook.get_sqlalchemy_engine(engine_kwargs)  # type: ignore
