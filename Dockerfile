FROM apache/airflow:2.7.1-python3.10
USER root
WORKDIR /airflow
COPY requirements.txt .
COPY airflow/rs_airflow_infras ./rs_airflow_infras
RUN pip install --no-cache-dir -r requirements.txt

COPY airflow/dags ./dags