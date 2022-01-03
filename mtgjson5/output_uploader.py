"""
S3 Uploader to store MTGJSON files in a Bucket
"""
import logging
import pathlib

import boto3
import botocore.exceptions


class MtgjsonUploader:
    """
    Upload MTGJSON compiled data to S3 Bucket
    """

    logger: logging.Logger
    s3_client: boto3.client

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.s3_client = boto3.client("s3")

    def upload_file(self, file_path: str, bucket_name: str, object_path: str) -> bool:
        """
        Upload a file to S3
        :param file_path: Path on local system to upload
        :param bucket_name: S3 Bucket to upload to
        :param object_path: Path in S3 Bucket to upload to
        :returns True if upload succeeded
        """
        try:
            self.s3_client.upload_file(file_path, bucket_name, object_path)
            self.logger.info(
                f"Successfully uploaded {file_path} to s3://{bucket_name}/{object_path}"
            )
            return True
        except botocore.exceptions.ClientError as error:
            self.logger.error(
                f"Failed to upload {file_path} to s3://{bucket_name}/{object_path}",
                error,
            )
        return False

    def upload_directory(self, directory_path: pathlib.Path, bucket_name: str) -> None:
        """
        Upload a directory to S3
        :param directory_path: Path on local system to recursively upload
        :param bucket_name: S3 Bucket to upload to
        """
        self.logger.info(f"Uploading {directory_path} contents to {bucket_name}")
        for item in directory_path.glob("**/*"):
            if item.is_file():
                self.upload_file(
                    str(item), bucket_name, str(item.relative_to(directory_path.parent))
                )
