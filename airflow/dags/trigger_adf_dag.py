from airflow.decorators import dag

from datetime import datetime

from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator


@dag("trigger_adf_dag", start_date=datetime(2023, 10, 3), schedule=None)
def trigger_adf_dag():
    AzureDataFactoryRunPipelineOperator(
        task_id="trigger_adf_dag", azure_data_factory_conn_id="adf_conn", pipeline_name="test_airflow"
    )


if __name__ == "__main__":
    trigger_adf_dag().test()
