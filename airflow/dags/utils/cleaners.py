"""
utils/cleaners.py

Per-entity data cleaning and standardization functions.
Operates on pandas DataFrames that have already passed validation.

Cleaning operations per entity:
  - Strip leading/trailing whitespace from all string columns.
  - Coerce types: timestamps → UTC datetime, numerics → correct precision.
  - Standardise case for categorical columns.
  - Derive computed columns (e.g. profit for orders).
"""

from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _strip_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Strip whitespace from all object (string) columns in place."""
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda c: c.str.strip())
    return df


def _to_utc_ts(series: pd.Series) -> pd.Series:
    """Parse a series to UTC-aware datetime, coercing errors to NaT."""
    return pd.to_datetime(series, utc=True, errors="coerce")


def _to_date(series: pd.Series) -> pd.Series:
    """Parse a series to Python date objects, coercing errors to None."""
    parsed = pd.to_datetime(series, errors="coerce")
    return parsed.dt.date.where(parsed.notna(), None)


def _to_numeric(series: pd.Series, decimals: int = 2) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").round(decimals)


def _to_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0).astype(int)


# ---------------------------------------------------------------------------
# Per-entity cleaners
# ---------------------------------------------------------------------------

def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    df = _strip_strings(df.copy())
    df["signup_date"] = _to_date(df["signup_date"])
    df["country"] = df["country"].str.upper()
    df["customer_segment"] = df["customer_segment"].str.title()
    df["acquisition_channel"] = df["acquisition_channel"].str.title()
    logger.info("Cleaned customers: %d rows", len(df))
    return df


def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    df = _strip_strings(df.copy())
    df["cost_price"] = _to_numeric(df["cost_price"])
    df["selling_price"] = _to_numeric(df["selling_price"])
    df["inventory_quantity"] = _to_int(df["inventory_quantity"])
    df["category"] = df["category"].str.title()
    logger.info("Cleaned products: %d rows", len(df))
    return df


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    df = _strip_strings(df.copy())
    df["order_timestamp"] = _to_utc_ts(df["order_timestamp"])
    df["unit_price"] = _to_numeric(df["unit_price"])
    df["total_amount"] = _to_numeric(df["total_amount"])
    df["quantity"] = _to_int(df["quantity"])
    df["order_status"] = df["order_status"].str.title()
    logger.info("Cleaned orders: %d rows", len(df))
    return df


def clean_payments(df: pd.DataFrame) -> pd.DataFrame:
    df = _strip_strings(df.copy())
    df["payment_date"] = _to_utc_ts(df["payment_date"])
    df["amount"] = _to_numeric(df["amount"])
    df["payment_status"] = df["payment_status"].str.title()
    logger.info("Cleaned payments: %d rows", len(df))
    return df


def clean_inventory(df: pd.DataFrame) -> pd.DataFrame:
    df = _strip_strings(df.copy())
    df["quantity_on_hand"] = _to_int(df["quantity_on_hand"])
    df["last_restock_date"] = _to_date(df["last_restock_date"])
    logger.info("Cleaned inventory: %d rows", len(df))
    return df


def clean_revenue(df: pd.DataFrame) -> pd.DataFrame:
    df = _strip_strings(df.copy())
    df["total_revenue"] = _to_numeric(df["total_revenue"])
    df["total_profit"] = _to_numeric(df["total_profit"])
    df["record_date"] = _to_date(df["record_date"])
    logger.info("Cleaned revenue: %d rows", len(df))
    return df


def clean_returns(df: pd.DataFrame) -> pd.DataFrame:
    df = _strip_strings(df.copy())
    df["return_date"] = _to_utc_ts(df["return_date"])
    df["refund_amount"] = _to_numeric(df["refund_amount"])
    df["return_reason"] = df["return_reason"].str.title()
    df["return_status"] = df["return_status"].str.title()
    logger.info("Cleaned returns: %d rows", len(df))
    return df


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

CLEANERS = {
    "customers": clean_customers,
    "products":  clean_products,
    "orders":    clean_orders,
    "payments":  clean_payments,
    "inventory": clean_inventory,
    "revenue":   clean_revenue,
    "returns":   clean_returns,
}


def clean(entity: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply entity-specific cleaning to a validated DataFrame.

    Returns
    -------
    Cleaned DataFrame ready for staging load.
    """
    fn = CLEANERS.get(entity)
    if fn is None:
        raise ValueError(f"No cleaner defined for entity: {entity!r}")
    return fn(df)
