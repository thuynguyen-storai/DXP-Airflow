from datetime import datetime

from airflow.datasets import Dataset
from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from rs_airflow.transfers.azure_blob_storage_to_snowflake import AzureBlobToSnowflake

transaction_header_csv = Dataset("blob://etl-data/SalesHeaderFCT*.txt")
staging_transaction_header_snowflake = Dataset("snowflake://STAGING.TRANSACTIONHEADER_RAW")
transaction_header_snowflake = Dataset("snowflake://PUBLIC.TRANSACTIONHEADER")


transaction_line_csv = Dataset("blob://etl-data/SalesLineFCT*.txt")
staging_transaction_line_snowflake = Dataset("snowflake://STAGING.TRANSACTIONLINES_RAW")
transaction_line_snowflake = Dataset("snowflake://PUBLIC.TRANSACTIONLINES")


@dag(
    dag_id="import_transaction",
    schedule=[transaction_header_csv, transaction_line_csv],
    start_date=datetime(2023, 11, 20),
    tags=["loyalty"],
)
def import_transaction():
    import_transaction_header_csv_to_sf_raw = AzureBlobToSnowflake(
        task_id="import_transaction_header_csv_to_sf_raw",
        source_blob_conn_id="azure_blob_conn",
        source_blob_container="etl-data",
        source_blob_name="SalesHeaderFCT.*[.]txt",
        dest_snowflake_conn_id="snowflake_conn",
        dest_schema="staging",
        dest_table="TransactionHeader_raw",
        input_file_config={"FIELD_DELIMITER ": "|", "PARSE_HEADER": True},
        copy_into_config={"MATCH_BY_COLUMN_NAME": "CASE_INSENSITIVE"},
        dest_prerun_query="TRUNCATE TABLE staging.TransactionHeader_raw",
        inlets=[transaction_header_csv],
        outlets=[staging_transaction_header_snowflake],
    )

    import_transaction_header_sf_raw_to_main = SQLExecuteQueryOperator(
        task_id="import_transaction_header_sf_raw_to_main",
        sql="./scripts/merge_transaction_header.sql",
        conn_id="snowflake_conn",
        autocommit=True,
        split_statements=False,
        show_return_value_in_logs=True,
        inlets=[staging_transaction_header_snowflake],
        outlets=[transaction_header_snowflake],
    )

    import_transaction_header_csv_to_sf_raw >> import_transaction_header_sf_raw_to_main

    import_transaction_lines_csv_to_sf_raw = AzureBlobToSnowflake(
        task_id="import_transaction_lines_csv_to_sf_raw",
        source_blob_conn_id="azure_blob_conn",
        source_blob_container="etl-data",
        source_blob_name="SalesLineFCT.*[.]txt",
        dest_snowflake_conn_id="snowflake_conn",
        dest_schema="staging",
        dest_table="TransactionLines_raw",
        input_file_config={"FIELD_DELIMITER ": "|", "PARSE_HEADER": True},
        copy_into_config={"MATCH_BY_COLUMN_NAME": "CASE_INSENSITIVE"},
        dest_prerun_query="TRUNCATE TABLE staging.TransactionLines_raw",
        inlets=[transaction_line_csv],
        outlets=[staging_transaction_line_snowflake],
    )

    import_transaction_lines_sf_raw_to_main = SQLExecuteQueryOperator(
        task_id="import_transaction_lines_sf_raw_to_main",
        sql="./scripts/merge_transaction_lines.sql",
        conn_id="snowflake_conn",
        autocommit=True,
        split_statements=False,
        show_return_value_in_logs=True,
        inlets=[staging_transaction_line_snowflake],
        outlets=[transaction_line_snowflake],
    )

    import_transaction_lines_csv_to_sf_raw >> import_transaction_lines_sf_raw_to_main


import_transaction()


if __name__ == "__main__":
    import_transaction().test()
