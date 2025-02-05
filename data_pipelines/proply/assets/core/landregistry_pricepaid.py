import time
from io import BytesIO
from typing import Any

import boto3
import requests
from dagster import AssetExecutionContext, EnvVar, asset

from ...common.resources.s3_resource import S3Resource

CHUNK_SIZE = 1024 * 1024 * 10


def download_url_stream(url):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
            yield chunk


@asset(io_manager_key="s3_to_postgres_io_manager")
def fetch_landregistry_pricepaid_from_url(
    context: AssetExecutionContext,
    s3: S3Resource,
) -> Any:
    """
    Collects landregistry price paid data in CSV and writes the output to
    the AWS S3 bucket
    """

    bucket = "proply"

    full_load = False
    if full_load:
        url = (
            "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com"
            "/pp-complete.csv"
        )
    else:
        url = (
            "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com"
            "/pp-monthly-update-new-version.csv"
        )
    current_datetimestamp = time.strftime("%Y%m%d-%H%M%S")
    s3_destination_path = (
        f"landing/landregistry/pricepaid/"
        f"landregistry_pricepaid_{current_datetimestamp}.csv"
    )

    bytes_stream = download_url_stream(url=url)

    s3.write_stream(
        bytes_stream=bytes_stream,
        bucket="proply",
        destination=s3_destination_path,
    )

    return {
        "s3_key": s3_destination_path,
        "target_table": "staging.landregistry_pricepaid",
        "delimiter": ",",
        "schema": {
            "transaction_id": "TEXT",
            "price_paid": "TEXT",
            "transaction_date": "TEXT",
            "address_postcode": "TEXT",
            "property_type": "TEXT",
            "new_build": "TEXT",
            "estate_type": "TEXT",
            "address_primary_object_name": "TEXT",
            "address_secondary_object_name": "TEXT",
            "address_street": "TEXT",
            "address_locality": "TEXT",
            "address_town": "TEXT",
            "address_district": "TEXT",
            "address_county": "TEXT",
            "transaction_category": "TEXT",
            "record_status": "TEXT",
        },
        "header": False,
    }
