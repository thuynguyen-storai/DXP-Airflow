from datetime import datetime

from rs_airflow.transfers.azure_blob_storage_to_sql_server import AzureBlobStorageToSqlServerOperator

from airflow.decorators import dag


@dag("azure_blob_to_sql_server_dag", start_date=datetime(2023, 10, 28), schedule=None)
def azure_blob_to_sql_server_dag():
    AzureBlobStorageToSqlServerOperator(az_blob_conn_id="azure_blob_conn", task_id="blob_to_sql")


azure_blob_to_sql_server_dag()

if __name__ == "__main__":
    azure_blob_to_sql_server_dag().test()
