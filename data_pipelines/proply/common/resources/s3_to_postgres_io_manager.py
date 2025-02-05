from typing import Any

from dagster import ConfigurableIOManager, InputContext, OutputContext

from .postgres_resource import PostgresResource
from .ssh_resource import SSHResource


class S3ToPostgresIOManager(ConfigurableIOManager):
    postgres_ssh_resource: SSHResource
    postgres_resource: PostgresResource

    def handle_output(
        self,
        context: OutputContext,
        obj: Any,
    ) -> None:
        """
        Expects obj to be a dict with:
        {
         's3_key': 'path/to/file.csv',
         'target_table': 'schema.table_name',
         'schema': 'dictionary of columnames and data types'
         'delimiter': ',', # optional, defaults to ','
         'header': bool, defaults to false
        }
        """
        s3_key = obj["s3_key"]
        target_table = obj["target_table"]
        delimiter = obj.get("delimiter", ",")
        schema = obj["schema"]
        header = obj.get("header", False)
        encoding = obj.get("encoding", "UTF8")

        ssh_client = self.postgres_ssh_resource.get_client()
        _, stdout, _ = ssh_client.exec_command(
            f"aws --endpoint=http://host.docker.internal:4566 s3 cp s3://proply/{s3_key} /tmp/staging/"
        )
        exit_status = stdout.channel.recv_exit_status()

        postgres_conn = self.postgres_resource.get_connection()
        cursor = postgres_conn.cursor()
        filename = s3_key[s3_key.rfind("/") + 1 :]

        cursor.execute(f"DROP TABLE IF EXISTS {target_table}")
        postgres_conn.commit()

        cursor.execute(
            f"CREATE TABLE {target_table} ("
            + ",".join(
                [f"{column} {data_type}" for column, data_type in schema.items()]
            )
            + ")"
        )
        postgres_conn.commit()

        pg_header_option = "HEADER" if header else ""
        cursor.execute(
            f"COPY {target_table} FROM '/tmp/staging/{filename}' DELIMITER '{delimiter}' CSV QUOTE '\"' {pg_header_option} ENCODING '{encoding}'"
        )
        postgres_conn.commit()

    def load_input(self, context: InputContext) -> Any:
        """
        This IO Manager is input only
        """
        raise NotImplementedError("This IO Manager only supports outputs")
