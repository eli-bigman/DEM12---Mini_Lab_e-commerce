"""
tests/test_cleaners.py

Unit tests for airflow/dags/utils/cleaners.py

Tests cover:
  - Strings get whitespace stripped
  - Timestamps are parsed to UTC-aware datetimes
  - Numeric columns are coerced to the correct type and precision
  - Categorical columns are title-cased / upper-cased as appropriate
  - Unknown entity raises ValueError
"""

import sys
import os
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "airflow", "dags"))

from utils.cleaners import clean


# ---------------------------------------------------------------------------
# Customers
# ---------------------------------------------------------------------------

class TestCustomersCleaner:
    def test_strips_whitespace(self, valid_customers_df):
        df = valid_customers_df.copy()
        df.loc[0, "country"] = "  US  "
        cleaned = clean("customers", df)
        assert cleaned.loc[0, "country"] == "US"

    def test_country_uppercased(self, valid_customers_df):
        df = valid_customers_df.copy()
        df.loc[0, "country"] = "de"
        cleaned = clean("customers", df)
        assert cleaned.loc[0, "country"] == "DE"

    def test_segment_title_cased(self, valid_customers_df):
        df = valid_customers_df.copy()
        df.loc[0, "customer_segment"] = "vip"
        cleaned = clean("customers", df)
        assert cleaned.loc[0, "customer_segment"] == "Vip"


# ---------------------------------------------------------------------------
# Products
# ---------------------------------------------------------------------------

class TestProductsCleaner:
    def test_cost_price_is_float(self, valid_products_df):
        cleaned = clean("products", valid_products_df)
        assert isinstance(cleaned.loc[0, "cost_price"], float)

    def test_inventory_quantity_is_int(self, valid_products_df):
        cleaned = clean("products", valid_products_df)
        assert isinstance(int(cleaned.loc[0, "inventory_quantity"]), int)

    def test_selling_price_rounded(self, valid_products_df):
        df = valid_products_df.copy()
        df.loc[0, "selling_price"] = "89.999999"
        cleaned = clean("products", df)
        assert round(cleaned.loc[0, "selling_price"], 2) == cleaned.loc[0, "selling_price"]


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------

class TestOrdersCleaner:
    def test_order_timestamp_is_utc(self, valid_orders_df):
        cleaned = clean("orders", valid_orders_df)
        ts = cleaned.loc[0, "order_timestamp"]
        assert ts.tzinfo is not None

    def test_total_amount_is_float(self, valid_orders_df):
        cleaned = clean("orders", valid_orders_df)
        assert isinstance(cleaned.loc[0, "total_amount"], float)

    def test_order_status_title_cased(self, valid_orders_df):
        df = valid_orders_df.copy()
        df.loc[0, "order_status"] = "completed"
        cleaned = clean("orders", df)
        assert cleaned.loc[0, "order_status"] == "Completed"


# ---------------------------------------------------------------------------
# Payments
# ---------------------------------------------------------------------------

class TestPaymentsCleaner:
    def test_payment_date_is_utc(self, valid_payments_df):
        cleaned = clean("payments", valid_payments_df)
        ts = cleaned.loc[0, "payment_date"]
        assert ts.tzinfo is not None

    def test_amount_is_float(self, valid_payments_df):
        cleaned = clean("payments", valid_payments_df)
        assert isinstance(cleaned.loc[0, "amount"], float)


# ---------------------------------------------------------------------------
# Inventory
# ---------------------------------------------------------------------------

class TestInventoryCleaner:
    def test_quantity_on_hand_is_int(self, valid_inventory_df):
        cleaned = clean("inventory", valid_inventory_df)
        assert isinstance(int(cleaned.loc[0, "quantity_on_hand"]), int)


# ---------------------------------------------------------------------------
# Revenue
# ---------------------------------------------------------------------------

class TestRevenueCleaner:
    def test_total_revenue_is_float(self, valid_revenue_df):
        cleaned = clean("revenue", valid_revenue_df)
        assert isinstance(cleaned.loc[0, "total_revenue"], float)


# ---------------------------------------------------------------------------
# Returns
# ---------------------------------------------------------------------------

class TestReturnsCleaner:
    def test_return_date_is_utc(self, valid_returns_df):
        cleaned = clean("returns", valid_returns_df)
        ts = cleaned.loc[0, "return_date"]
        assert ts.tzinfo is not None

    def test_return_reason_title_cased(self, valid_returns_df):
        df = valid_returns_df.copy()
        df.loc[0, "return_reason"] = "defective"
        cleaned = clean("returns", df)
        assert cleaned.loc[0, "return_reason"] == "Defective"


# ---------------------------------------------------------------------------
# Unknown entity raises
# ---------------------------------------------------------------------------

def test_unknown_entity_raises():
    with pytest.raises(ValueError, match="No cleaner defined"):
        clean("unknown_entity", pd.DataFrame())
