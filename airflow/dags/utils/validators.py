"""
utils/validators.py

Great Expectations-based validation for each entity DataFrame.
Uses the GX Ephemeral DataContext (in-memory, no filesystem required).

For each entity, the validator returns:
  - valid_df   : DataFrame of rows that passed all expectations
  - invalid_df : DataFrame of rows that failed one or more expectations
"""

from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------

def _split_valid_invalid(df: pd.DataFrame, mask: pd.Series) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split DataFrame into (passing, failing) based on a boolean mask."""
    return df[mask].copy(), df[~mask].copy()


def _base_uuid_mask(df: pd.DataFrame, col: str) -> pd.Series:
    """Return a boolean Series: True if the column looks like a UUID."""
    import re
    uuid_re = re.compile(
        r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
        re.IGNORECASE,
    )
    return df[col].astype(str).str.match(uuid_re)


def _not_null_mask(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    """Return True for rows where all cols are non-null and non-empty string."""
    mask = pd.Series(True, index=df.index)
    for col in cols:
        mask &= df[col].notna() & (df[col].astype(str).str.strip() != "")
    return mask


def _is_positive_numeric(series: pd.Series) -> pd.Series:
    """Return True for rows where the series can be parsed as a positive number."""
    numeric = pd.to_numeric(series, errors="coerce")
    return numeric.notna() & (numeric >= 0)


def _no_duplicates_mask(df: pd.DataFrame, id_col: str) -> pd.Series:
    """Return True for the first occurrence of each id; flags subsequent duplicates."""
    return ~df.duplicated(subset=[id_col], keep="first")


# ---------------------------------------------------------------------------
# Per-entity validation functions
# ---------------------------------------------------------------------------

def validate_customers(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    logger.info("Validating customers (%d rows)", len(df))
    required = ["customer_id", "signup_date", "country", "acquisition_channel", "customer_segment"]
    valid_channels = {"Organic", "Facebook", "Google", "Email"}
    valid_segments = {"New", "Returning", "VIP"}

    mask = (
        _not_null_mask(df, required)
        & _base_uuid_mask(df, "customer_id")
        & df["acquisition_channel"].isin(valid_channels)
        & df["customer_segment"].isin(valid_segments)
        & _no_duplicates_mask(df, "customer_id")
    )
    return _split_valid_invalid(df, mask)


def validate_products(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    logger.info("Validating products (%d rows)", len(df))
    required = ["product_id", "product_name", "category", "cost_price", "selling_price", "inventory_quantity"]

    mask = (
        _not_null_mask(df, required)
        & _base_uuid_mask(df, "product_id")
        & _is_positive_numeric(df["cost_price"])
        & _is_positive_numeric(df["selling_price"])
        & _is_positive_numeric(df["inventory_quantity"])
        & _no_duplicates_mask(df, "product_id")
    )
    return _split_valid_invalid(df, mask)


def validate_orders(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    logger.info("Validating orders (%d rows)", len(df))
    required = ["order_id", "customer_id", "product_id", "quantity", "unit_price", "total_amount", "order_timestamp"]
    valid_statuses = {"Completed", "Cancelled", "Refunded"}

    mask = (
        _not_null_mask(df, required)
        & _base_uuid_mask(df, "order_id")
        & _is_positive_numeric(df["unit_price"])
        & _is_positive_numeric(df["total_amount"])
        & _is_positive_numeric(df["quantity"])
        & df["order_status"].isin(valid_statuses)
        & _no_duplicates_mask(df, "order_id")
    )
    return _split_valid_invalid(df, mask)


def validate_payments(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    logger.info("Validating payments (%d rows)", len(df))
    required = ["payment_id", "order_id", "payment_date", "payment_method", "amount", "payment_status"]
    valid_statuses = {"Successful", "Failed", "Pending"}

    mask = (
        _not_null_mask(df, required)
        & _base_uuid_mask(df, "payment_id")
        & _is_positive_numeric(df["amount"])
        & df["payment_status"].isin(valid_statuses)
        & _no_duplicates_mask(df, "payment_id")
    )
    return _split_valid_invalid(df, mask)


def validate_inventory(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    logger.info("Validating inventory (%d rows)", len(df))
    required = ["inventory_id", "product_id", "warehouse_location", "quantity_on_hand", "last_restock_date"]

    mask = (
        _not_null_mask(df, required)
        & _base_uuid_mask(df, "inventory_id")
        & _is_positive_numeric(df["quantity_on_hand"])
        & _no_duplicates_mask(df, "inventory_id")
    )
    return _split_valid_invalid(df, mask)


def validate_revenue(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    logger.info("Validating revenue (%d rows)", len(df))
    required = ["revenue_id", "record_date", "total_revenue", "total_profit"]

    mask = (
        _not_null_mask(df, required)
        & _base_uuid_mask(df, "revenue_id")
        & _is_positive_numeric(df["total_revenue"])
        & _is_positive_numeric(df["total_profit"])
        & _no_duplicates_mask(df, "revenue_id")
    )
    return _split_valid_invalid(df, mask)


def validate_returns(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    logger.info("Validating returns (%d rows)", len(df))
    required = ["return_id", "order_id", "return_date", "return_reason", "refund_amount", "return_status"]
    valid_reasons = {"Defective", "Wrong Item", "Changed Mind"}
    valid_statuses = {"Processed", "Pending", "Rejected"}

    mask = (
        _not_null_mask(df, required)
        & _base_uuid_mask(df, "return_id")
        & _is_positive_numeric(df["refund_amount"])
        & df["return_reason"].isin(valid_reasons)
        & df["return_status"].isin(valid_statuses)
        & _no_duplicates_mask(df, "return_id")
    )
    return _split_valid_invalid(df, mask)


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

VALIDATORS = {
    "customers": validate_customers,
    "products":  validate_products,
    "orders":    validate_orders,
    "payments":  validate_payments,
    "inventory": validate_inventory,
    "revenue":   validate_revenue,
    "returns":   validate_returns,
}


def validate(entity: str, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validate a DataFrame for the given entity.

    Returns
    -------
    (valid_df, invalid_df)
    """
    fn = VALIDATORS.get(entity)
    if fn is None:
        raise ValueError(f"No validator defined for entity: {entity!r}")
    valid, invalid = fn(df)
    logger.info(
        "Validation complete for %s: %d valid, %d invalid",
        entity, len(valid), len(invalid),
    )
    return valid, invalid
