"""
uploader.py

Handles uploading local CSV files to MinIO under time-partitioned paths.

Path structure:  raw-data/{entity}/YYYY/MM/DD/{filename}
"""

import io
import logging
import os
from datetime import datetime, timezone

from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)


def get_minio_client() -> Minio:
    """Initialise and return a MinIO client from environment variables."""
    endpoint = os.environ["MINIO_ENDPOINT"]
    access_key = os.environ["MINIO_ROOT_USER"]
    secret_key = os.environ["MINIO_ROOT_PASSWORD"]

    client = Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )
    logger.info("MinIO client initialised for endpoint: %s", endpoint)
    return client


def upload_csv(
    client: Minio,
    bucket: str,
    entity: str,
    filename: str,
    csv_content: str,
    timestamp: datetime | None = None,
) -> str:
    """
    Upload a CSV string to MinIO under a time-partitioned object path.

    Parameters
    ----------
    client      : MinIO client instance
    bucket      : Target bucket name (e.g. 'raw-data')
    entity      : Entity name used as prefix (e.g. 'orders')
    filename    : The CSV filename (e.g. 'orders_20240301_120000.csv')
    csv_content : Raw CSV string content to upload
    timestamp   : The datetime used to build the partition path. Defaults to UTC now.

    Returns
    -------
    Full object path in MinIO.
    """
    if timestamp is None:
        timestamp = datetime.now(tz=timezone.utc)

    year = timestamp.strftime("%Y")
    month = timestamp.strftime("%m")
    day = timestamp.strftime("%d")

    object_path = f"{entity}/{year}/{month}/{day}/{filename}"

    data = csv_content.encode("utf-8")
    data_stream = io.BytesIO(data)
    data_length = len(data)

    try:
        client.put_object(
            bucket_name=bucket,
            object_name=object_path,
            data=data_stream,
            length=data_length,
            content_type="text/csv",
        )
        logger.info("Uploaded: s3://%s/%s", bucket, object_path)
        return object_path
    except S3Error as exc:
        logger.error("Failed to upload %s: %s", object_path, exc)
        raise
