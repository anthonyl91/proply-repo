import os
import time
from io import BytesIO
from typing import Any

from dagster import AssetExecutionContext, EnvVar, asset
from stream_unzip import stream_unzip

from proply.common.resources.postgres_resource import PostgresResource
from proply.common.resources.ssh_resource import SSHResource

from ...common.resources.s3_resource import S3Resource

CHUNK_SIZE = 1024 * 1024 * 10


def get_download_url_stream(url):
    with open(os.path.expanduser("~/Downloads/bdline_essh_gb.zip"), "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            yield chunk


def unzipped_stream(bytes_stream):
    for file_name, _, unzipped_chunks in stream_unzip(bytes_stream):
        if file_name[-4:] == b"westminster_const_region.shp":
            yield from unzipped_chunks
        else:
            for _ in unzipped_chunks:
                pass


def chunk_bytestream(bytes_stream):
    current_chunk_size = 0
    current_chunk_bytes = BytesIO()
    for chunk in bytes_stream:
        current_chunk_size += len(chunk)
        if current_chunk_size > CHUNK_SIZE:
            current_chunk_bytes.write(chunk[: current_chunk_size - CHUNK_SIZE])
            current_chunk_bytes.seek(0)
            yield current_chunk_bytes.read()
            current_chunk_bytes = BytesIO()
            current_chunk_bytes.write(chunk[current_chunk_size - CHUNK_SIZE :])
            current_chunk_size = 0
        else:
            current_chunk_bytes.write(chunk)
    current_chunk_bytes.seek(0)
    yield current_chunk_bytes.read()


def create_postgres_table(postgres_resource: PostgresResource) -> None:
    postgres_conn = postgres_resource.get_connection()
    cursor = postgres_conn.cursor()

    postgres_create_statement = """
        -- Table: postgis.ordinancesurvey_boundarylines

        DROP TABLE IF EXISTS postgis.ordinancesurvey_boundarylines;
        DROP SEQUENCE IF EXISTS ordinancesurvey_boundarylines_gid_seq;
        CREATE SEQUENCE ordinancesurvey_boundarylines_gid_seq
            AS integer
            START WITH 1
            INCREMENT BY 1
            NO MINVALUE
            NO MAXVALUE
            CACHE 1;
        CREATE TABLE IF NOT EXISTS postgis.ordinancesurvey_boundarylines
        (
            gid integer NOT NULL DEFAULT nextval('ordinancesurvey_boundarylines_gid_seq'::regclass),
            name character varying(100) COLLATE pg_catalog."default",
            area_code character varying(3) COLLATE pg_catalog."default",
            descriptio character varying(50) COLLATE pg_catalog."default",
            file_name character varying(100) COLLATE pg_catalog."default",
            "number" double precision,
            number0 double precision,
            polygon_id double precision,
            unit_id double precision,
            code character varying(9) COLLATE pg_catalog."default",
            hectares double precision,
            area double precision,
            type_code character varying(2) COLLATE pg_catalog."default",
            descript0 character varying(200) COLLATE pg_catalog."default",
            type_cod0 character varying(3) COLLATE pg_catalog."default",
            descript1 character varying(36) COLLATE pg_catalog."default",
            geom geometry(MultiPolygon),
            CONSTRAINT ordinancesurvey_boundarylines_pkey PRIMARY KEY (gid)
        )

        TABLESPACE pg_default;

        ALTER TABLE IF EXISTS postgis.ordinancesurvey_boundarylines
            OWNER to postgres;
    """
    cursor.execute(postgres_create_statement)
    postgres_conn.commit()


def load_shpfiles_to_postgres(
    postgres_ssh_resource: SSHResource, directory: str
) -> None:
    ssh_client = postgres_ssh_resource.get_client()
    bash_script_string = f"""
        export PGPASSWORD={EnvVar("PG_PASSWORD").get_value()}
        shopt -s globstar
        for file in {directory}/Data/GB/*_region.shp;
        do
          filepath="${{file%.*}}"
          echo $filepath
          shp2pgsql -a -s SRID $file postgis.ordinancesurvey_boundarylines \\
          | psql -h localhost -d proply -U postgres
        done
    """
    _, stdout, _ = ssh_client.exec_command(bash_script_string)
    exit_status = stdout.channel.recv_exit_status()


@asset
def fetch_ordinancesurvey_boundarylines_from_url(
    context: AssetExecutionContext,
    s3: S3Resource,
    postgres_ssh_resource: SSHResource,
    postgres_resource: PostgresResource,
) -> Any:
    download_stream = get_download_url_stream(None)
    bytes_stream = chunk_bytestream(download_stream)
    current_datetimestamp = time.strftime("%Y%m%d-%H%M%S")
    s3_destination_path = f"staging/ordinancesurvey/boundarylines/ordinancesurvey_boundarylines_{current_datetimestamp}.zip"
    s3.write_stream(
        bytes_stream=bytes_stream,
        bucket="proply",
        destination=s3_destination_path,
    )
    # Copy zip to Postgres Server
    ssh_client = postgres_ssh_resource.get_client()
    _, stdout, _ = ssh_client.exec_command(
        f"aws --endpoint=http://host.docker.internal:4566 s3 cp s3://proply/{s3_destination_path} /tmp/staging/"
    )
    stdout.channel.recv_exit_status()
    # unzip file
    filename = s3_destination_path[s3_destination_path.rfind("/") + 1 :]
    ssh_client = postgres_ssh_resource.get_client()
    _, stdout, _ = ssh_client.exec_command(
        f"unzip /tmp/staging/{filename} -d /tmp/staging/{filename[:-4]}"
    )
    stdout.channel.recv_exit_status()
    # Run ssh shp2pgis
    create_postgres_table(postgres_resource)
    load_shpfiles_to_postgres(postgres_ssh_resource, f"/tmp/staging/{filename[:-4]}")
    return {
        "s3_key": s3_destination_path,
        "target_table": "postgis.ordinancesurvey_boundarylines",
    }
