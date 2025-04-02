from dagster import ConfigurableResource, get_dagster_logger
from pydantic import Field
from enum import Enum
from pathlib import Path
import os
import json
from typing import BinaryIO, Dict, List, Optional, Union, Any

logger = get_dagster_logger()


class StorageType(str, Enum):
    LOCAL = "local"
    S3 = "s3"


class StorageResource(ConfigurableResource):
    """Resource for handling file storage, either local or S3."""

    storage_type: StorageType
    local_base_path: Optional[str] = None
    s3_bucket_name: Optional[str] = None

    def validate_config(self) -> None:
        """Validate storage configuration"""
        if self.storage_type == StorageType.LOCAL and not self.local_base_path:
            raise ValueError("local_base_path must be provided for local storage")
        if self.storage_type == StorageType.S3 and not self.s3_bucket_name:
            raise ValueError("s3_bucket_name must be provided for S3 storage")

    def setup_for_execution(self, context) -> None:
        """Initialize storage"""
        self.validate_config()
        if self.storage_type == StorageType.LOCAL:
            os.makedirs(self.local_base_path, exist_ok=True)

    def list_files(self, folder_path: str, extension: str = None) -> List[str]:
        """List files in a folder with optional extension filter."""
        if self.storage_type == StorageType.LOCAL:
            base_dir = Path(self.local_base_path) / folder_path
            if not base_dir.exists():
                logger.warning(f"Directory {base_dir} does not exist.")
                return []
            files = list(base_dir.glob(f"*{extension if extension else ''}"))
            return [
                str(f.relative_to(Path(self.local_base_path)))
                for f in files
                if f.is_file()
            ]
        elif self.storage_type == StorageType.S3:
            import boto3

            s3_client = boto3.client("s3")
            try:
                response = s3_client.list_objects_v2(
                    Bucket=self.s3_bucket_name, Prefix=folder_path
                )
                if "Contents" not in response:
                    return []
                return [
                    item["Key"]
                    for item in response["Contents"]
                    if not extension or item["Key"].endswith(extension)
                ]
            except Exception as e:
                logger.error(f"Error listing S3 files: {e}")
                return []

    def read_file(self, file_path: str) -> BinaryIO:
        """Read a file from storage."""
        if self.storage_type == StorageType.LOCAL:
            full_path = Path(self.local_base_path) / file_path
            return open(full_path, "rb")
        elif self.storage_type == StorageType.S3:
            import boto3

            s3_client = boto3.client("s3")
            import io

            data = io.BytesIO()
            try:
                s3_client.download_fileobj(self.s3_bucket_name, file_path, data)
                data.seek(0)
                return data
            except Exception as e:
                logger.error(f"Error reading S3 file {file_path}: {e}")
                raise

    def write_file(
        self, file_path: str, content: Union[bytes, str, Dict[str, Any]]
    ) -> str:
        """Write content to a file in storage."""
        if self.storage_type == StorageType.LOCAL:
            full_path = Path(self.local_base_path) / file_path
            os.makedirs(full_path.parent, exist_ok=True)
            mode = "wb" if isinstance(content, bytes) else "w"
            with open(full_path, mode) as f:
                if isinstance(content, (dict, list)):
                    json.dump(content, f)
                else:
                    f.write(content)
            return str(full_path)
        elif self.storage_type == StorageType.S3:
            import boto3

            s3_client = boto3.client("s3")
            try:
                if isinstance(content, (dict, list)):
                    content = json.dumps(content).encode("utf-8")
                elif isinstance(content, str):
                    content = content.encode("utf-8")
                s3_client.put_object(
                    Bucket=self.s3_bucket_name, Key=file_path, Body=content
                )
                return f"s3://{self.s3_bucket_name}/{file_path}"
            except Exception as e:
                logger.error(f"Error writing to S3 file {file_path}: {e}")
                raise

    def read_json(self, file_path: str) -> Dict[str, Any]:
        """Read JSON file from storage."""
        if self.storage_type == StorageType.LOCAL:
            full_path = Path(self.local_base_path) / file_path
            with open(full_path) as f:
                return json.load(f)
        elif self.storage_type == StorageType.S3:
            import boto3

            s3_client = boto3.client("s3")
            try:
                response = s3_client.get_object(
                    Bucket=self.s3_bucket_name, Key=file_path
                )
                return json.loads(response["Body"].read().decode("utf-8"))
            except Exception as e:
                logger.error(f"Error reading S3 JSON file {file_path}: {e}")
                raise

    def get_full_path(self, subfolder: str) -> str:
        """Get the full path for a subfolder based on storage type."""
        if self.storage_type == StorageType.LOCAL:
            if not self.local_base_path:
                raise ValueError("local_base_path must be set for local storage")
            full_path = Path(self.local_base_path) / subfolder
            full_path.mkdir(parents=True, exist_ok=True)
            return str(full_path)
        elif self.storage_type == StorageType.S3:
            if not self.s3_bucket_name:
                raise ValueError("s3_bucket_name must be set for S3 storage")
            return f"s3://{self.s3_bucket_name}/{subfolder}"
        else:
            raise ValueError(f"Unsupported storage type: {self.storage_type}")
