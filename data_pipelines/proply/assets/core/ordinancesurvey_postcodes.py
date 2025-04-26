import time
from io import BytesIO
from typing import Any

import requests
from dagster import AssetExecutionContext, asset
from stream_unzip import stream_unzip

from ...common.resources.s3_resource import S3Resource

CHUNK_SIZE = 1024 * 1024 * 10


def download_url_stream(url):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
            yield chunk


def unzipped_stream(bytes_stream):
    for file_name, _, unzipped_chunks in stream_unzip(bytes_stream):
        if b"Data/CSV" in file_name:
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


@asset(io_manager_key="s3_to_postgres_io_manager")
def fetch_ordinancesurvey_postcodes_from_url(
    context: AssetExecutionContext,
    s3: S3Resource,
) -> Any:
    url = "https://api.os.uk/downloads/v1/products/CodePointOpen/downloads?area=GB&format=CSV&redirect"
    download_stream = download_url_stream(url)
    bytes_stream = chunk_bytestream(unzipped_stream(download_stream))
    current_datetimestamp = time.strftime("%Y%m%d-%H%M%S")
    s3_destination_path = f"staging/ordinancesurvey/postcodes/ordinancesurvey_postcodes_{current_datetimestamp}.csv"
    s3.write_stream(
        bytes_stream=bytes_stream, bucket="proply", destination=s3_destination_path
    )

    return {
        "s3_key": s3_destination_path,
        "target_table": "staging.ordinancesurvey_postcodes",
        "delimiter": ",",
        "schema": {
            "postcode": "TEXT",
            "positional_quality_indicator": "TEXT",
            "eastings": "TEXT",
            "northings": "TEXT",
            "country_code": "TEXT",
            "nhs_regional_ha_code": "TEXT",
            "nhs_ha_code": "TEXT",
            "admin_county_code": "TEXT",
            "admin_district_code": "TEXT",
            "admin_ward_code": "TEXT",
        },
    }
