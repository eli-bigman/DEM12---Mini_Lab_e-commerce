"""Ingest stage task helpers used by the ecommerce pipeline DAG."""

from __future__ import annotations

import logging
import os

from tasks.clean import clean
from tasks.load import load_to_staging
from tasks.validate import validate
from utils.db_helper import file_already_processed, mark_file_processed
from utils.minio_helper import get_client, list_objects, read_csv, send_to_quarantine

logger = logging.getLogger(__name__)


def discover_new_files(entity: str) -> dict:
    """Discover new files in MinIO for this entity that haven't been processed."""
    bucket = os.environ.get("MINIO_RAW_BUCKET", "raw-data")
    client = get_client()

    all_paths = list_objects(client, bucket, prefix=f"{entity}/")
    new_paths = [p for p in all_paths if not file_already_processed(p)]

    logger.info("Entity=%s: discovered %d new file(s) to process", entity, len(new_paths))
    return {
        "entity": entity,
        "file_paths": new_paths,
        "file_count": len(new_paths),
    }


def validate_and_quarantine(entity: str, file_paths: list) -> dict:
    """Validate all files and route invalid rows to quarantine."""
    if not file_paths:
        logger.info("Entity=%s: no files to validate", entity)
        return {
            "entity": entity,
            "valid_rows": 0,
            "invalid_rows": 0,
            "quarantined_files": 0,
        }

    bucket = os.environ.get("MINIO_RAW_BUCKET", "raw-data")
    client = get_client()

    total_valid = 0
    total_invalid = 0
    quarantined_file_count = 0

    for obj_path in file_paths:
        file_invalid = 0

        for chunk_idx, df in enumerate(read_csv(client, bucket, obj_path)):
            valid_df, invalid_df = validate(entity, df)

            if not invalid_df.empty:
                send_to_quarantine(client, entity, obj_path, invalid_df, chunk_idx)
                file_invalid += len(invalid_df)
                quarantined_file_count += 1

            total_valid += len(valid_df)
            total_invalid += file_invalid

    logger.info(
        "Entity=%s validation complete: %d valid, %d invalid rows",
        entity, total_valid, total_invalid,
    )
    return {
        "entity": entity,
        "valid_rows": total_valid,
        "invalid_rows": total_invalid,
        "quarantined_files": quarantined_file_count,
    }


def clean_data(entity: str, file_paths: list) -> dict:
    """Clean valid rows from files (no DB writes in this stage)."""
    if not file_paths:
        logger.info("Entity=%s: no files to clean", entity)
        return {
            "entity": entity,
            "cleaned_rows": 0,
            "failed_files": 0,
        }

    bucket = os.environ.get("MINIO_RAW_BUCKET", "raw-data")
    client = get_client()

    total_cleaned = 0
    failed_file_count = 0

    for obj_path in file_paths:
        try:
            file_cleaned = 0

            for _, df in enumerate(read_csv(client, bucket, obj_path)):
                valid_df, _ = validate(entity, df)
                if valid_df.empty:
                    continue

                clean_df = clean(entity, valid_df)
                file_cleaned += len(clean_df)

            if file_cleaned == 0:
                logger.warning("File %s had no valid rows to clean", obj_path)

            total_cleaned += file_cleaned

        except Exception as exc:
            logger.error("Failed cleaning %s: %s", obj_path, exc, exc_info=True)
            failed_file_count += 1

    logger.info(
        "Entity=%s cleaning complete: %d rows cleaned, %d files failed",
        entity, total_cleaned, failed_file_count,
    )
    return {
        "entity": entity,
        "cleaned_rows": total_cleaned,
        "failed_files": failed_file_count,
    }


def load_to_staging_db(entity: str, file_paths: list) -> dict:
    """Load cleaned rows to staging and mark files as processed."""
    if not file_paths:
        logger.info("Entity=%s: no files to load", entity)
        return {
            "entity": entity,
            "loaded_rows": 0,
            "failed_files": 0,
        }

    bucket = os.environ.get("MINIO_RAW_BUCKET", "raw-data")
    client = get_client()

    total_loaded = 0
    failed_file_count = 0

    for obj_path in file_paths:
        try:
            file_loaded = 0

            for _, df in enumerate(read_csv(client, bucket, obj_path)):
                valid_df, _ = validate(entity, df)
                if valid_df.empty:
                    continue

                clean_df = clean(entity, valid_df)
                load_to_staging(entity, clean_df, source_file=obj_path)
                file_loaded += len(clean_df)

            if file_loaded == 0:
                logger.warning("File %s had no valid rows to load", obj_path)

            total_loaded += file_loaded

            mark_file_processed(
                obj_path,
                entity,
                row_count=file_loaded,
                quarantine_count=0,
                status="success" if file_loaded > 0 else "empty",
            )

        except Exception as exc:
            logger.error("Failed loading %s: %s", obj_path, exc, exc_info=True)
            failed_file_count += 1

    logger.info(
        "Entity=%s loading complete: %d rows loaded, %d files failed",
        entity, total_loaded, failed_file_count,
    )
    return {
        "entity": entity,
        "loaded_rows": total_loaded,
        "failed_files": failed_file_count,
    }
