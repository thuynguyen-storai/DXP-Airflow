from datetime import datetime
from rs_airflow.transfers.azure_blob_storage_to_snowflake import AzureBlobToSnowflake


from airflow.decorators import dag


@dag("azure_blob_to_snowflake_dag", start_date=datetime(2023, 10, 28), schedule=None)
def azure_blob_to_snowflake_dag():
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


azure_blob_to_snowflake_dag()

if __name__ == "__main__":
    azure_blob_to_snowflake_dag().test()
