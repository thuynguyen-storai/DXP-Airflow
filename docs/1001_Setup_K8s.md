# Setup Kubernetes - steps by steps

## Build Docker image

```shell
docker build -t ducthuyng/test-airflow-rs-image:0.0.2 -t ducthuyng/test-airflow-rs-image .
```

## Upload newly created image to Hub

```shell
docker push ducthuyng/test-airflow-rs-image
docker push ducthuyng/test-airflow-rs-image:0.0.2
```

## Update Helm Chart

Install Airflow Helm repository

```shell
helm repo add apache-airflow https://airflow.apache.org
```

```shell
helm upgrade --install airflow apache-airflow/airflow --values ./k8s/SandboxR1.values.yaml --namespace airflow --debug
```

