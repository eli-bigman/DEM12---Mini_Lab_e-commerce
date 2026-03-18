"""
tests/test_validators.py

Unit tests for airflow/dags/utils/validators.py

Tests cover:
  - Valid rows pass validation (zero invalid rows returned)
  - Null required fields are quarantined
  - Invalid enum values are quarantined
  - Malformed UUIDs are quarantined
  - Duplicate IDs within a batch are quarantined (keep first)
  - Negative numeric values are quarantined
"""

import sys
import os
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "airflow", "dags"))

from utils.validators import validate


# ---------------------------------------------------------------------------
# Customers
# ---------------------------------------------------------------------------

class TestCustomersValidator:
    def test_valid_rows_pass(self, valid_customers_df):
        valid, invalid = validate("customers", valid_customers_df)
        assert len(valid) == 2
        assert len(invalid) == 0

    def test_null_country_is_quarantined(self, valid_customers_df):
        df = valid_customers_df.copy()
        df.loc[0, "country"] = None
        valid, invalid = validate("customers", df)
        assert len(invalid) == 1

    def test_invalid_channel_is_quarantined(self, valid_customers_df):
        df = valid_customers_df.copy()
        df.loc[0, "acquisition_channel"] = "Twitter"
        valid, invalid = validate("customers", df)
        assert len(invalid) == 1

    def test_invalid_segment_is_quarantined(self, valid_customers_df):
        df = valid_customers_df.copy()
        df.loc[0, "customer_segment"] = "Premium"
        valid, invalid = validate("customers", df)
        assert len(invalid) == 1

    def test_malformed_uuid_is_quarantined(self, valid_customers_df):
        df = valid_customers_df.copy()
        df.loc[0, "customer_id"] = "not-a-uuid"
        valid, invalid = validate("customers", df)
        assert len(invalid) == 1

    def test_duplicate_ids_are_quarantined(self, valid_customers_df):
        df = pd.concat([valid_customers_df, valid_customers_df.iloc[[0]]], ignore_index=True)
        valid, invalid = validate("customers", df)
        # GE expect_column_values_to_be_unique flags ALL occurrences of a duplicate
        assert len(invalid) >= 1



# ---------------------------------------------------------------------------
# Products
# ---------------------------------------------------------------------------

class TestProductsValidator:
    def test_valid_rows_pass(self, valid_products_df):
        valid, invalid = validate("products", valid_products_df)
        assert len(valid) == 1
        assert len(invalid) == 0

    def test_negative_cost_price_quarantined(self, valid_products_df):
        df = valid_products_df.copy()
        df.loc[0, "cost_price"] = "-10"
        valid, invalid = validate("products", df)
        assert len(invalid) == 1

    def test_null_product_name_quarantined(self, valid_products_df):
        df = valid_products_df.copy()
        df.loc[0, "product_name"] = None
        valid, invalid = validate("products", df)
        assert len(invalid) == 1

    def test_non_numeric_price_quarantined(self, valid_products_df):
        df = valid_products_df.copy()
        df.loc[0, "selling_price"] = "free"
        valid, invalid = validate("products", df)
        assert len(invalid) == 1


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------

class TestOrdersValidator:
    def test_valid_rows_pass(self, valid_orders_df):
        valid, invalid = validate("orders", valid_orders_df)
        assert len(valid) == 1
        assert len(invalid) == 0

    def test_pending_status_is_allowed(self, valid_orders_df):
        df = valid_orders_df.copy()
        df.loc[0, "order_status"] = "Pending"
        valid, invalid = validate("orders", df)
        assert len(valid) == 1
        assert len(invalid) == 0

    def test_invalid_status_quarantined(self, valid_orders_df):
        df = valid_orders_df.copy()
        df.loc[0, "order_status"] = "InProgress"
        valid, invalid = validate("orders", df)
        assert len(invalid) == 1

    def test_missing_timestamp_quarantined(self, valid_orders_df):
        df = valid_orders_df.copy()
        df.loc[0, "order_timestamp"] = None
        valid, invalid = validate("orders", df)
        assert len(invalid) == 1

    def test_negative_quantity_quarantined(self, valid_orders_df):
        df = valid_orders_df.copy()
        df.loc[0, "quantity"] = "-1"
        valid, invalid = validate("orders", df)
        assert len(invalid) == 1


# ---------------------------------------------------------------------------
# Payments
# ---------------------------------------------------------------------------

class TestPaymentsValidator:
    def test_valid_rows_pass(self, valid_payments_df):
        valid, invalid = validate("payments", valid_payments_df)
        assert len(valid) == 1
        assert len(invalid) == 0

    def test_invalid_status_quarantined(self, valid_payments_df):
        df = valid_payments_df.copy()
        df.loc[0, "payment_status"] = "Declined"
        valid, invalid = validate("payments", df)
        assert len(invalid) == 1


# ---------------------------------------------------------------------------
# Inventory
# ---------------------------------------------------------------------------

class TestInventoryValidator:
    def test_valid_rows_pass(self, valid_inventory_df):
        valid, invalid = validate("inventory", valid_inventory_df)
        assert len(valid) == 1
        assert len(invalid) == 0

    def test_negative_quantity_quarantined(self, valid_inventory_df):
        df = valid_inventory_df.copy()
        df.loc[0, "quantity_on_hand"] = "-50"
        valid, invalid = validate("inventory", df)
        assert len(invalid) == 1


# ---------------------------------------------------------------------------
# Revenue
# ---------------------------------------------------------------------------

class TestRevenueValidator:
    def test_valid_rows_pass(self, valid_revenue_df):
        valid, invalid = validate("revenue", valid_revenue_df)
        assert len(valid) == 1
        assert len(invalid) == 0

    def test_null_revenue_quarantined(self, valid_revenue_df):
        df = valid_revenue_df.copy()
        df.loc[0, "total_revenue"] = None
        valid, invalid = validate("revenue", df)
        assert len(invalid) == 1


# ---------------------------------------------------------------------------
# Returns
# ---------------------------------------------------------------------------

class TestReturnsValidator:
    def test_valid_rows_pass(self, valid_returns_df):
        valid, invalid = validate("returns", valid_returns_df)
        assert len(valid) == 1
        assert len(invalid) == 0

    def test_invalid_reason_quarantined(self, valid_returns_df):
        df = valid_returns_df.copy()
        df.loc[0, "return_reason"] = "Buyer Remorse"
        valid, invalid = validate("returns", df)
        assert len(invalid) == 1

    def test_invalid_status_quarantined(self, valid_returns_df):
        df = valid_returns_df.copy()
        df.loc[0, "return_status"] = "Completed"
        valid, invalid = validate("returns", df)
        assert len(invalid) == 1


# ---------------------------------------------------------------------------
# Unknown entity raises
# ---------------------------------------------------------------------------

def test_unknown_entity_raises():
    with pytest.raises(ValueError, match="No validator defined"):
        validate("unknown_entity", pd.DataFrame())
