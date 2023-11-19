import importlib.metadata

__version__ = importlib.metadata.version("rs_airflow")


# This is needed to allow Airflow to pick up specific metadata fields it needs for certain features.
def get_provider_info():
    return {
        "package-name": "rs-airflow",
        "name": "RS Airflow",
        "description": "DXP-Airflow customized libraries",
        "hooks": [
            {
                "integration-name": "StreamSets",
                "python-modules": ["rs_airflow.streamsets.hooks.StreamsetsHook"],
            },
            {
                "integration-name": "RS ElasticsearchHook",
                "python-modules": [
                    "rs_airflow.elasticsearch.hooks.RsElasticsearchHook"
                ],
            },
        ],
        "connection-types": [
            {
                "hook-class-name": "rs_airflow.streamsets.hooks.StreamsetsHook",
                "connection-type": "streamsets",
            },
            {
                "hook-class-name": "rs_airflow.elasticsearch.hooks.RsElasticsearchHook",
                "connection-type": "rs_elasticsearch",
            },
        ],
        "versions": [__version__],
    }
