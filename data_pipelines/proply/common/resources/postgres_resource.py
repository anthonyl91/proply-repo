import psycopg2
from dagster import ConfigurableResource


class PostgresResource(ConfigurableResource):
    host: str
    port: int
    database: str
    username: str
    password: str

    def get_connection(self):
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.username,
            password=self.password,
        )
        # conn.set_isolation_level(psycopg2.extentions.ISOLATION_LEVEL_AUTOCOMMIT)
        return conn
