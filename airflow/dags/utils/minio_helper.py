"""
utils/minio_helper.py

Helpers for interacting with MinIO (S3-compatible object storage).
Provides functions to list unprocessed files, read CSV content,
and move invalid records to the quarantine bucket (DLQ).
"""

import io
import logging
import os
from datetime import datetime, timezone

import pandas as pd
from minio import Minio

logger = logging.getLogger(__name__)

QUARANTINE_BUCKET = "quarantine"


def get_client() -> Minio:
    """Return an authenticated MinIO client from environment variables."""
    return Minio(
        endpoint=os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ROOT_USER"],
        secret_key=os.environ["MINIO_ROOT_PASSWORD"],
        secure=False,
    )


def list_objects(client: Minio, bucket: str, prefix: str = "") -> list[str]:
    """
    Return all object paths under `prefix` in `bucket`.

    Parameters
    ----------
    client : MinIO client
    bucket : Bucket name
    prefix : Object prefix filter (e.g. 'orders/')

    Returns
    -------
    List of full object paths.
    """
    objects = client.list_objects(bucket, prefix=prefix, recursive=True)
    return [obj.object_name for obj in objects]


def read_csv(client: Minio, bucket: str, object_path: str) -> pd.DataFrame:
    """
    Download a CSV object from MinIO and return it as a pandas DataFrame.

    Parameters
    ----------
    client      : MinIO client
    bucket      : Source bucket name
    object_path : Full object path within the bucket

    Returns
    -------
    pandas DataFrame with all columns as strings (type enforcement comes later).
    """
    response = client.get_object(bucket, object_path)
    try:
        raw = response.read()
    finally:
        response.close()
        response.release_conn()

    df = pd.read_csv(io.BytesIO(raw), dtype=str)
    logger.info("Read %d rows from s3://%s/%s", len(df), bucket, object_path)
    return df


def send_to_quarantine(
    client: Minio,
    entity: str,
    source_path: str,
    df: pd.DataFrame,
) -> str:
    """
    Upload a DataFrame of rejected rows to the quarantine bucket.

    The quarantine path mirrors the source path:
        quarantine/{entity}/YYYY/MM/DD/{original_filename}_quarantine.csv

    Parameters
    ----------
    client      : MinIO client
    entity      : Entity name (e.g. 'orders')
    source_path : Original MinIO object path (used to derive quarantine path)
    df          : DataFrame of invalid rows to quarantine

    Returns
    -------
    Full quarantine object path.
    """
    now = datetime.now(tz=timezone.utc)
    filename = source_path.split("/")[-1].replace(".csv", "_quarantine.csv")
    quarantine_path = (
        f"{entity}/{now.strftime('%Y/%m/%d')}/{filename}"
    )

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    client.put_object(
        bucket_name=QUARANTINE_BUCKET,
        object_name=quarantine_path,
        data=io.BytesIO(csv_bytes),
        length=len(csv_bytes),
        content_type="text/csv",
    )
    logger.warning(
        "Quarantined %d rows from %s → s3://%s/%s",
        len(df), source_path, QUARANTINE_BUCKET, quarantine_path,
    )
    return quarantine_path
