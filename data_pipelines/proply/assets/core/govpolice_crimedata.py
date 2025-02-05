import json
import re
import time
from io import BytesIO
from typing import Any

import boto3
import httpx
import requests
from bs4 import BeautifulSoup
from dagster import AssetExecutionContext, EnvVar, asset
from stream_unzip import stream_unzip

from ...common.resources.s3_resource import S3Resource

CHUNK_SIZE = 1024 * 1024 * 10


def get_dataset_metadata_from_site(request_session, url):
    response = request_session.get(url)
    response_soup = BeautifulSoup(response.content, features="html.parser")
    # Get start and end dates for download request
    dates_select_element = response_soup.find("select", {"name": "date_to"})
    date_list = [
        option.get("value") for option in dates_select_element.find_all("option")
    ]

    # Get all forces options
    forces_select_element = response_soup.find("ul", {"id": "id_forces"})
    forces_list = [
        list_item.get("value") for list_item in forces_select_element.find_all("input")
    ]

    csrftoken = response.cookies.get_dict()["csrftoken"]

    return {"date_list": date_list, "forces_list": forces_list, "csrftoken": csrftoken}


def get_max_extraction_date_from_pg():
    # TODO: Implement later
    return "2024-10"


def get_request_payload(dataset_metadata, max_extract_record_date):
    date_list = dataset_metadata["date_list"]
    min_date = None
    if max_extract_record_date is None:
        min_date = min(date_list)
    date_index = date_list.index(max_extract_record_date)
    if date_index is None:
        min_date = min(date_list)
    else:
        index = date_list.index(max_extract_record_date)
        if index == len(date_list) - 1:
            min_date = date_list[index]
        else:
            min_date = date_list[index + 1]
    payload = {
        "csrfmiddlewaretoken": dataset_metadata["csrftoken"],
        "date_from": min_date,
        "date_to": max(dataset_metadata["date_list"]),
        "forces": dataset_metadata["forces_list"],
        "include_crime": "on",
        # "include_outcomes": "on",
        # "include_stop_and_search": "on",
    }
    return payload


def request_download_dataset_url(request_session, url, headers, payload):
    response = request_session.post(f"{url}/data/", headers=headers, data=payload)

    progress_url = re.search(
        r"fetch_download\(\\'(.*?)\/\\'\)", str(response.content)
    ).group(1)

    progress_status = None
    retries = 0
    while progress_status == None or progress_status != "ready":
        if retries == 100:
            # TODO: Raise err - timed out - 1000 seconds
            break
        progress_response_content = requests.get(f"{url}{progress_url}").content
        progress_response_json = json.loads(progress_response_content)
        progress_status = progress_response_json["status"]

        retries += 1
        time.sleep(10)

    download_url = progress_response_json["url"]
    return download_url


def download_stream(url):

    with requests.get(url, stream=True) as download_response:
        for zip_chunk in download_response.iter_content(chunk_size=CHUNK_SIZE):
            yield from BytesIO(zip_chunk)


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


def unzipped_stream(url):
    for _, _, unzipped_chunks in stream_unzip(download_stream(url)):
        yield from unzipped_chunks


@asset(io_manager_key="s3_to_postgres_io_manager")
def fetch_govpolice_crimedata_from_url(
    context: AssetExecutionContext,
    s3: S3Resource,
) -> Any:
    """
    Collects crimedata from the UK police website, downloads the CSV and
    writes the output to the AWS S3 bucket
    """
    url = "https://data.police.uk"
    request_session = requests.session()
    dataset_metadata = get_dataset_metadata_from_site(request_session, f"{url}/data/")

    max_record_extraction_date = get_max_extraction_date_from_pg()
    request_payload = get_request_payload(dataset_metadata, max_record_extraction_date)
    request_headers = {"Referer": f"{url}/data/"}

    if request_payload["date_to"] <= max_record_extraction_date:
        # Most recent data has already been ingested
        return

    download_url = request_download_dataset_url(
        request_session, url, request_headers, request_payload
    )

    current_datetimestamp = time.strftime("%Y%m%d-%H%M%S")
    s3_destination_path = (
        f"landing/govpolice/crimedata/"
        f"govpolice_crimedata_{current_datetimestamp}.csv"
    )

    raw_file_bytestream = chunk_bytestream(unzipped_stream(download_url))

    s3.write_stream(
        bytes_stream=raw_file_bytestream,
        bucket="proply",
        destination=s3_destination_path,
    )

    return {
        "s3_key": s3_destination_path,
        "target_table": "staging.govpolice_crimedata",
        "delimiter": ",",
        "schema": {
            "crime_id": "TEXT",
            "month": "TEXT",
            "reported_by": "TEXT",
            "falls_within": "TEXT",
            "longitude": "TEXT",
            "latitude": "TEXT",
            "location": "TEXT",
            "lsoa_code": "TEXT",
            "lsoa_name": "TEXT",
            "crime_type": "TEXT",
            "last_outcome_category": "TEXT",
            "context": "TEXT",
        },
        "header": True,
    }
