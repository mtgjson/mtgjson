"""
S3 Uploader to store MTGJSON files in a Bucket
"""

import logging
import pathlib
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

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

    def download_file(self, bucket_name: str, bucket_object_path: str, local_save_file_path: str) -> bool:
        """
        Download a file from S3
        :param bucket_name: Bucket to get file from
        :param bucket_object_path: Path within Bucket file resides at
        :param local_save_file_path: Where to save the file on the local system
        :returns Did download complete successfully
        """
        try:
            self.s3_client.download_file(bucket_name, bucket_object_path, local_save_file_path)
            return True
        except botocore.exceptions.ClientError as error:
            self.logger.error(f"Failed to download s3://{bucket_name}/{bucket_object_path}: {error}")
            return False

    def upload_file(
        self,
        local_file_path: str,
        bucket_name: str,
        bucket_object_path: str,
        tags: dict[str, str] | None = None,
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

            self.s3_client.upload_file(local_file_path, bucket_name, bucket_object_path, ExtraArgs=extra_args)
            self.logger.info(f"Successfully uploaded {local_file_path} to s3://{bucket_name}/{bucket_object_path}")
            return True
        except botocore.exceptions.ClientError as error:
            self.logger.error(f"Failed to upload {local_file_path} to s3://{bucket_name}/{bucket_object_path}: {error}")
        return False

    def upload_file_with_retry(
        self,
        local_file_path: str,
        bucket_name: str,
        bucket_object_path: str,
        tags: dict[str, str] | None = None,
        cache_ttl_sec: int = 86400,
        max_retries: int = 3,
        base_delay: float = 1.0,
    ) -> bool:
        """
        Upload a file to S3 with retry logic and exponential backoff.
        :param local_file_path: Path on local system to upload
        :param bucket_name: S3 Bucket to upload to
        :param bucket_object_path: Path in S3 Bucket to upload to
        :param tags: Tags to upload with
        :param cache_ttl_sec: How long to tell browsers to cache the file for (Default: 1 day)
        :param max_retries: Maximum number of retry attempts (default: 3)
        :param base_delay: Base delay in seconds for exponential backoff (default: 1.0)
        :returns True if upload succeeded
        """
        for attempt in range(max_retries + 1):
            if self.upload_file(local_file_path, bucket_name, bucket_object_path, tags, cache_ttl_sec):
                return True

            if attempt < max_retries:
                delay = base_delay * (2**attempt)
                self.logger.warning(f"Retry {attempt + 1}/{max_retries} for {local_file_path} after {delay}s delay")
                time.sleep(delay)

        self.logger.error(f"Failed to upload {local_file_path} after {max_retries + 1} attempts")
        return False

    def upload_directory(
        self,
        directory_path: pathlib.Path,
        bucket_name: str,
        tags: dict[str, str] | None = None,
        max_workers: int = 16,
        max_retries: int = 3,
    ) -> None:
        """
        Upload a directory to S3 in parallel with retry logic.
        :param directory_path: Path on local system to recursively upload
        :param bucket_name: S3 Bucket to upload to
        :param tags: Tags to upload each file with
        :param max_workers: Maximum number of concurrent uploads (default: 16)
        :param max_retries: Maximum number of retry attempts per file (default: 3)
        """
        files_to_upload = [item for item in directory_path.glob("**/*") if item.is_file()]
        total_files = len(files_to_upload)
        self.logger.info(
            f"Uploading {total_files} files from {directory_path} to {bucket_name} with {max_workers} workers"
        )

        successful = 0
        failed = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    self.upload_file_with_retry,
                    str(item),
                    bucket_name,
                    str(item.relative_to(directory_path.parent)),
                    tags,
                    86400,
                    max_retries,
                ): item
                for item in files_to_upload
            }

            for future in as_completed(futures):
                item = futures[future]
                try:
                    if future.result():
                        successful += 1
                    else:
                        failed += 1
                except Exception as e:
                    self.logger.error(f"Unexpected error uploading {item}: {e}")
                    failed += 1

        if failed > 0:
            raise RuntimeError(f"Upload incomplete: {failed}/{total_files} files failed after retries")

        self.logger.info(f"Upload complete: {successful}/{total_files} files uploaded")
