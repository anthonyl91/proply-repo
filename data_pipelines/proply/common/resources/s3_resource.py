from typing import Generator

import boto3
from dagster import ConfigurableResource


class S3Resource(ConfigurableResource):
    aws_access_key_id: str
    aws_secret_access_key: str
    endpoint_url: str

    def get_client(self):
        """
        Returns an boto3 aws s3 client session
        """
        aws_session = boto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )
        s3_client = aws_session.client(
            service_name="s3", endpoint_url=self.endpoint_url, use_ssl=True
        )
        return s3_client

    def write_stream(
        self,
        bytes_stream: Generator[bytes, None, None],
        bucket: str,
        destination: str,
    ):
        """
        write_stream takes a stream of bytes and stream writes the
        bytes to an s3 object
        """

        s3_client = self.get_client()

        part_number = 1
        parts = []

        response = s3_client.create_multipart_upload(
            Bucket=bucket, ContentType="text/html", Key=destination
        )
        upload_id = response["UploadId"]

        for chunk in bytes_stream:
            part = s3_client.upload_part(
                Bucket=bucket,
                Key=destination,
                Body=chunk,
                PartNumber=part_number,
                UploadId=upload_id,
            )
            parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
            part_number += 1

        s3_client.complete_multipart_upload(
            Bucket=bucket,
            Key=destination,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

    def read_file(self, bucket, path):
        s3_client = self.get_client()
        response = s3_client.get_object(Bucket=bucket, Key=path)
        return response["Body"]
