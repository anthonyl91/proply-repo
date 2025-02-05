import csv
import time
from io import BytesIO, StringIO
from typing import Any

import boto3
import requests
from bs4 import BeautifulSoup
from dagster import AssetExecutionContext, EnvVar, asset
from stream_unzip import stream_unzip

from ...common.resources.s3_resource import S3Resource

CHUNK_SIZE = 1024 * 1024 * 10


def get_download_form_details(url):
    response = requests.get(url)
    response_soup = BeautifulSoup(response.content, features="html.parser")
    form_element = response_soup.find("form", {"action": "/Downloads/Collate"})

    form_parameters = {
        input["name"]: input["value"]
        for input in form_element.find_all("input")
        if input.has_attr("name") and "Selected" not in input["name"]
    }
    form_action_url = form_element["action"]
    form_action_method = form_element["method"]

    establishment_fields_name_key = form_element.find(
        lambda element: element.name == "label"
        and "Establishment fields" in element.text
    )["for"]

    form_parameters[establishment_fields_name_key] = "true"

    return {
        "url": form_action_url,
        "method": form_action_method,
        "parameters": form_parameters,
        "cookies": response.cookies,
    }


def request_file_download_url(url, method, parameters, cookies):
    response = requests.post(url=url, data=parameters, cookies=cookies)
    download_status_url = response.url
    retries = 0
    while retries < 100:
        download_status_response = requests.get(download_status_url)
        response_soup = BeautifulSoup(
            download_status_response.content, features="html.parser"
        )
        download_extract_form_element = response_soup.find(
            "form", {"action": "/Downloads/Download/Extract"}
        )
        if download_extract_form_element is not None:
            form_action_url = download_extract_form_element["action"]
            form_action_method = download_extract_form_element["method"]
            form_parameters = {
                input["name"]: input["value"]
                for input in download_extract_form_element.find_all("input")
                if input.has_attr("name")
            }

            return {
                "url": form_action_url,
                "method": form_action_method,
                "parameters": form_parameters,
                "cookies": download_status_response.cookies,
            }
        time.sleep(10)
        retries += 1


def download_stream(url, method, parameters, cookies):
    with requests.request(
        method, url, data=parameters, cookies=cookies, stream=True
    ) as download_response:
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


def unzipped_stream(bytes_stream):
    for _, _, unzipped_chunks in stream_unzip(bytes_stream):
        yield from unzipped_chunks


def byte_stream_to_string(byte_stream):
    for chunk in byte_stream:
        yield chunk.decode("cp1252")


def process_csv(file_stream, columns):
    string_stream = byte_stream_to_string(file_stream.iter_lines())
    csv_reader = csv.reader(string_stream, delimiter=",", quotechar='"')
    header = next(csv_reader)
    column_index_arr = [header.index(col) for col in columns]

    csv_buffer = StringIO()
    csv_writer = csv.writer(csv_buffer, delimiter=",", quotechar='"')
    for row in csv_reader:
        csv_writer = csv.writer(csv_buffer, delimiter=",", quotechar='"')
        processed_row = [row[index] for index in column_index_arr]
        csv_writer.writerow(processed_row)
        csv_buffer.seek(0)
        yield csv_buffer.read().encode("utf-8")
        csv_buffer.truncate(0)
        csv_buffer.seek(0)


@asset(io_manager_key="s3_to_postgres_io_manager")
def fetch_govschool_establishmentdata_from_url(
    context: AssetExecutionContext,
    s3: S3Resource,
) -> Any:
    """
    Collects govschools estahblishment data, downloads CSV and writes output to the AWS S3 bucket
    """

    url = "https://get-information-schools.service.gov.uk"
    download_form_details = get_download_form_details(f"{url}/Downloads")
    download_url = request_file_download_url(
        f"{url}{download_form_details["url"]}",
        download_form_details["method"],
        download_form_details["parameters"],
        download_form_details["cookies"],
    )

    bytes_stream = chunk_bytestream(
        unzipped_stream(
            download_stream(
                url=f"{url}{download_url["url"]}",
                method=download_url["method"],
                parameters=download_url["parameters"],
                cookies=download_url["cookies"],
            )
        )
    )
    current_datetimestamp = time.strftime("%Y%m%d-%H%M%S")
    s3_destination_path = (
        f"landing/govschools/establishmentdata/"
        f"govschools_establishmentdata_{current_datetimestamp}.csv"
    )

    s3.write_stream(
        bytes_stream=bytes_stream,
        bucket="proply",
        destination=s3_destination_path,
    )

    processed_csv_s3_destination_path = (
        f"staging/govschools/establishmentdata/"
        f"govschools_establishmentdata_{current_datetimestamp}.csv"
    )
    processed_csv_bytes_steam = chunk_bytestream(
        process_csv(
            s3.read_file(bucket="proply", path=s3_destination_path),
            columns=[
                "URN",
                "LA (code)",
                "LA (name)",
                "EstablishmentNumber",
                "EstablishmentName",
            ],
        )
    )
    s3.write_stream(
        bytes_stream=processed_csv_bytes_steam,
        bucket="proply",
        destination=processed_csv_s3_destination_path,
    )
    return {
        "s3_key": processed_csv_s3_destination_path,
        "target_table": "staging.govschools_establishmentdata",
        "delimiter": ",",
        "schema": {
            "urn": "TEXT",
            "la_code": "TEXT",
            "la_name": "TEXT",
            "establishment_number": "TEXT",
            "establishment_name": "TEXT",
        },
    }
