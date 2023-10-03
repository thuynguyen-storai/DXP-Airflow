FROM apache/airflow:2.7.1-python3.10
USER root
WORKDIR /airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt