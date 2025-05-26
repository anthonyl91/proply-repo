import json

from dagster import AssetExecutionContext, asset

from ...common.resources.postgres_resource import PostgresResource
from ...common.resources.s3_resource import S3Resource


@asset(deps=["stg_ordinancesurvey_postcodes"])
def export_postcode_geojson(
    context: AssetExecutionContext,
    postgres_resource: PostgresResource,
    s3: S3Resource,
):
    postgres_conn = postgres_resource.get_connection()
    cursor = postgres_conn.cursor()
    cursor.execute(
        """
        SELECT jsonb_build_object(
            'type',     'FeatureCollection',
            'features', jsonb_agg(feature)
        )
        FROM (
            SELECT jsonb_build_object(
                'type',       'Feature',
                'geometry',   ST_AsGeoJSON(ST_Simplify(geom, 0.0001))::jsonb,
                'properties', to_jsonb(row) - 'geom'
            ) AS feature
            FROM (
                SELECT gid, name, geom  -- Replace with your table and columns
                FROM postgis.ordinancesurvey_boundarylines
                WHERE geom IS NOT NULL
                limit 10
            ) row
        ) features;
        """
    )
    result = cursor.fetchone()
    # Since the query retuns only a single row - write the row to a file
    if not result:
        raise Exception("Postgis returned no geojson object")

    geojson_string = str(result[0])

    # Copy geojson to remote S3
    s3_client = s3.get_client()
    s3_client.put_object(
        Bucket="proply",
        Key="web/resources",
        Body=geojson_string,
        ContentType="application/json",
        ContentEncoding="utf-8",
    )
