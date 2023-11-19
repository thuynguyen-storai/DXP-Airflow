FROM apache/airflow:slim-2.7.3-python3.10

USER root

RUN apt update \
    && apt install -y --no-install-recommends openjdk-11-jre-headless procps git \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/

USER airflow
WORKDIR /airflow

# Install RS-Airflow
COPY --chown=airflow rs_airflow ./rs_airflow
COPY --chown=airflow requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


COPY --chown=airflow airflow/dags ./dags
COPY --chown=airflow airflow/airflow.cfg ./
COPY --chown=airflow airflow/webserver_config.py ./