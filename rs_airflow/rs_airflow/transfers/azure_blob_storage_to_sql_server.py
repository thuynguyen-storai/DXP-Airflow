import datetime
from typing import Any

from azure.storage.blob import BlobSasPermissions, generate_blob_sas

from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.utils.context import Context


class AzureBlobStorageToSqlServerOperator(BaseOperator):
    """
    Experiment: This operator simply read all data from a SQL Table and INDEX it to Elasticsearch

    Args:
        table_schema (str): Schema of SQL Table (e.g. `dbo`, `public`)
        table_name (str): SQL table name
        id_column (str): Column name that would be used as ID for Elasticsearch document
        sql_conn_id (str): Connection ID of SQL Table.\
            When using with providers, this could be Snowflake, SQL Server, etc.
        elastic_conn_id (str): Connection ID of Elasticsearch
        elastic_index_name (str): Name of the destination index
        batch_size (int): How many records to be processed per batch (default is 1000)
        num_of_process (int): As this use a multiprocess.Pool at its core,\
            how many process should be allocated to this (default is 4)
        use_process_pool_over_thread_pool (bool): Whether to use ThreadPool or ProcessPool.\
            Experimentations show that it is best to keep using ThreadPool (which is the default value: False)
    """

    def __init__(
        self,
        az_blob_conn_id: str,
        # table_schema: str,
        # table_name: str,
        # id_column: str,
        # sql_conn_id: str,
        # elastic_conn_id: str,
        # elastic_index_name: str,
        # batch_size=1000,
        # num_of_processes=4,
        # use_process_pool_over_thread_pool=False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.az_blob_hook: WasbHook = BaseHook.get_connection(
            conn_id=az_blob_conn_id
        ).get_hook()

    def execute(self, context: Context) -> Any:
        blob_service_client = self.az_blob_hook.get_conn()

        blob_client = blob_service_client.get_blob_client(
            "dev-etl", "CampaignDIM_full_20221210.txt"
        )

        blob_sas_token = generate_blob_sas(
            str(blob_client.account_name),
            blob_client.container_name,
            blob_client.blob_name,
            account_key=blob_service_client.credential.account_key,
            expiry=datetime.datetime.now() + datetime.timedelta(minutes=15),
            permission=BlobSasPermissions(read=True),
        )
        print(f"{blob_client.url}?{blob_sas_token}")
