from airflow.exceptions import AirflowFailException
from airflow.models.connection import Connection


class JdbcConfigManager:
    """
    A class for converting Airflow - SQLAlchemy connection to JDBC necessary configs for connection
    """

    @classmethod
    def generate_jdbc_configs_for_sqlalchemy(
        cls, connection: Connection
    ) -> dict[str, str]:
        connection_generator_map = {
            "snowflake": JdbcConfigManager._generate_snowflake_config,
            "mssql": JdbcConfigManager._generate_mssql_config,
        }

        generate_func = connection_generator_map.get(connection.conn_type, None)

        if not generate_func:
            raise AirflowFailException(
                "Unknown connection type: %s", connection.conn_type
            )

        return generate_func(connection)

    @classmethod
    def _generate_snowflake_config(cls, connection: Connection) -> dict[str, str]:
        connection_extra = connection.extra_dejson

        sf_url = f"jdbc:snowflake://{connection_extra['account']}.{connection_extra['region']}.snowflakecomputing.com/"

        return {
            "url": sf_url,
            "user": f"{connection.login}",
            "password": f"{connection.get_password()}",
            "db": connection_extra["database"],
            "warehouse": connection_extra["warehouse"],
            "role": connection_extra["role"],
            "driver": "net.snowflake.client.jdbc.SnowflakeDriver",
        }

    @classmethod
    def _generate_mssql_config(cls, connection: Connection) -> dict[str, str]:
        port = int(connection.port) or 1433
        database_param = ""
        if str(connection.schema):
            database_param = f"databaseName={connection.schema}"

        extra_params = str.join(";", [database_param, "encrypt=false"])

        return {
            "url": f"jdbc:sqlserver://{connection.host}:{port};{extra_params}",
            "user": f"{connection.login}",
            "password": f"{connection.get_password()}",
            "db": connection.schema,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        }
