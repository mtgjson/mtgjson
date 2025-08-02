"""
S3 Uploader to store MTGJSON files in a Bucket
"""

import logging
import pathlib
import urllib.parse
from typing import Dict, Optional

import boto3
import botocore.exceptions


class MtgjsonS3Handler:
    """
    Upload MTGJSON compiled data to S3 Bucket
    """

    logger: logging.Logger
    s3_client: boto3.client

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.s3_client = boto3.client("s3")

    def download_file(
        self, bucket_name: str, bucket_object_path: str, local_save_file_path: str
    ) -> bool:
        """
        Download a file from S3
        :param bucket_name: Bucket to get file from
        :param bucket_object_path: Path within Bucket file resides at
        :param local_save_file_path: Where to save the file on the local system
        :returns Did download complete successfully
        """
        try:
            self.s3_client.download_file(
                bucket_name, bucket_object_path, local_save_file_path
            )
            return True
        except botocore.exceptions.ClientError as error:
            self.logger.error(
                f"Failed to download s3://{bucket_name}/{bucket_object_path}: {error}"
            )
            return False

    def upload_file(
        self,
        local_file_path: str,
        bucket_name: str,
        bucket_object_path: str,
        tags: Optional[Dict[str, str]] = None,
        cache_ttl_sec: int = 86400,
    ) -> bool:
        """
        Upload a file to S3
        :param local_file_path: Path on local system to upload
        :param bucket_name: S3 Bucket to upload to
        :param bucket_object_path: Path in S3 Bucket to upload to
        :param tags: Tags to upload with
        :param cache_ttl_sec: How long to tell browsers to cache the file for (Default: 1 day)
        :returns True if upload succeeded
        """
        try:
            extra_args = {"CacheControl": f"max-age={cache_ttl_sec}"}
            if tags:
                extra_args["Tagging"] = urllib.parse.urlencode(tags)

            self.s3_client.upload_file(
                local_file_path, bucket_name, bucket_object_path, ExtraArgs=extra_args
            )
            self.logger.info(
                f"Successfully uploaded {local_file_path} to s3://{bucket_name}/{bucket_object_path}"
            )
            return True
        except botocore.exceptions.ClientError as error:
            self.logger.error(
                f"Failed to upload {local_file_path} to s3://{bucket_name}/{bucket_object_path}: {error}"
            )
        return False

    def upload_directory(
        self,
        directory_path: pathlib.Path,
        bucket_name: str,
        tags: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Upload a directory to S3
        :param directory_path: Path on local system to recursively upload
        :param bucket_name: S3 Bucket to upload to
        :param tags: Tags to upload each file with
        """
        self.logger.info(f"Uploading {directory_path} contents to {bucket_name}")
        for item in directory_path.glob("**/*"):
            if item.is_file():
                self.upload_file(
                    str(item),
                    bucket_name,
                    str(item.relative_to(directory_path.parent)),
                    tags,
                )
