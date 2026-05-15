import json
from io import BytesIO
from typing import Generator

from dagster import AssetExecutionContext, asset
from psycopg2._psycopg import connection

from ...common.resources.postgres_resource import PostgresResource
from ...common.resources.s3_resource import S3Resource

PAGE_SIZE = 100
CHUNK_SIZE = 1024 * 1024 * 10


def _get_geojson_features(
    postgres_conn: connection,
) -> Generator[tuple[str, str], None, None]:

    result_page = 0
    while True:

        cursor = postgres_conn.cursor()
        cursor.execute(
            f"""
            SELECT ST_AsGeoJSON(ST_Simplify(geom,1000)) as geometry,
               to_jsonb(row) - 'geom' as properties
            FROM (
                SELECT gid, name, geom  -- Replace with your table and columns
                FROM postgis.ordinancesurvey_boundarylines
                WHERE geom IS NOT NULL
                ORDER BY gid
                LIMIT {PAGE_SIZE}
                OFFSET {PAGE_SIZE * result_page}
            ) row
            """
        )

        result = cursor.fetchall()
        if len(result) == 0:
            break

        for row in result:
            yield (row[0], json.dumps(row[1]))

        result_page += 1
        # For testing
        if result_page > 100:
            break


def _get_geojson_object_byte_stream(geojson_features) -> Generator[bytes, None, None]:

    # Output the starting GEO Json Object
    geojson_file_start = """
    {
      "type": "FeatureCollection",
      "features": [
    """
    yield geojson_file_start.encode()

    # Output the geojson features
    for index, (geojson_geometry, geojson_properties) in enumerate(geojson_features):
        geojson_feature = f"""
            {{
                "type": "Feature",
                "geometry": {geojson_geometry},
                "properties": {geojson_properties}
            }}
        """
        if index == 0:
            yield geojson_feature.encode()
        else:
            yield ("," + geojson_feature).encode()

    # Output the ending GEO Json Object
    geojson_file_end = """
        ]
    }
    """
    yield geojson_file_end.encode()


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


@asset(deps=["stg_ordinancesurvey_postcodes"])
def export_postcode_geojson(
    context: AssetExecutionContext,
    postgres_resource: PostgresResource,
    s3: S3Resource,
):
    postgres_conn = postgres_resource.get_connection()
    geojson_features = _get_geojson_features(postgres_conn=postgres_conn)

    geojson_byte_stream = chunk_bytestream(
        bytes_stream=_get_geojson_object_byte_stream(geojson_features=geojson_features)
    )
    s3_destination_path = "web/resources/geojson"
    s3.write_stream(
        bytes_stream=geojson_byte_stream,
        bucket="proply",
        destination=s3_destination_path,
    )
    # Copy geojson to remote S3
    # s3_client = s3.get_client()
    # s3_client.put_object(
    #    Bucket="proply",
    #    Key="web/resources/geojson",
    #    Body=geojson_string,
    #    ContentType="application/json",
    #    ContentEncoding="utf-8",
    # )
