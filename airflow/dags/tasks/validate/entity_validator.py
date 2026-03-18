"""
tasks/validate/entity_validator.py

Great Expectations-based validation for each entity DataFrame.
Uses an ephemeral GX DataContext (in-memory, no filesystem required).
"""

from __future__ import annotations

import logging

import great_expectations as gx
import pandas as pd

logger = logging.getLogger(__name__)

UUID_REGEX = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"


def _run_gx_suite(
	df: pd.DataFrame,
	entity: str,
	add_expectations_fn,
) -> tuple[pd.DataFrame, pd.DataFrame]:
	"""Run a Great Expectations suite and split valid/invalid rows."""
	context = gx.get_context(mode="ephemeral")
	source = context.sources.add_pandas(f"{entity}_source")
	asset = source.add_dataframe_asset(f"{entity}_asset")
	batch_request = asset.build_batch_request(dataframe=df)

	context.add_expectation_suite(f"{entity}_suite")
	validator = context.get_validator(
		batch_request=batch_request,
		expectation_suite_name=f"{entity}_suite",
	)

	add_expectations_fn(validator)
	result = validator.validate(
		result_format={"result_format": "COMPLETE", "partial_unexpected_count": 0},
	)

	failed_indices: set[int] = set()
	for expectation_result in result.results:
		if not expectation_result.success:
			idx_list = expectation_result.result.get("unexpected_index_list", [])
			failed_indices.update(idx_list)
			logger.debug(
				"GX [%s] failed expectation '%s': %d unexpected rows",
				entity,
				expectation_result.expectation_config.expectation_type,
				len(idx_list),
			)

	failed_mask = df.index.isin(failed_indices)
	valid_df = df[~failed_mask].copy()
	invalid_df = df[failed_mask].copy()

	logger.info(
		"GX validation complete for '%s': %d valid, %d invalid (suite success=%s)",
		entity,
		len(valid_df),
		len(invalid_df),
		result.success,
	)
	return valid_df, invalid_df


def validate_customers(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
	def add_expectations(v):
		for col in ["customer_id", "signup_date", "country", "acquisition_channel", "customer_segment"]:
			v.expect_column_values_to_not_be_null(col)
		v.expect_column_values_to_match_regex("customer_id", UUID_REGEX)
		v.expect_column_values_to_be_in_set(
			"acquisition_channel", ["Organic", "Facebook", "Google", "Email"]
		)
		v.expect_column_values_to_be_in_set("customer_segment", ["New", "Returning", "VIP"])
		v.expect_column_values_to_be_unique("customer_id")

	return _run_gx_suite(df, "customers", add_expectations)


def validate_products(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
	def add_expectations(v):
		for col in ["product_id", "product_name", "category", "cost_price", "selling_price", "inventory_quantity"]:
			v.expect_column_values_to_not_be_null(col)
		v.expect_column_values_to_match_regex("product_id", UUID_REGEX)
		v.expect_column_values_to_match_regex("cost_price", r"^\d+(\.\d+)?$")
		v.expect_column_values_to_match_regex("selling_price", r"^\d+(\.\d+)?$")
		v.expect_column_values_to_match_regex("inventory_quantity", r"^\d+$")
		v.expect_column_values_to_be_unique("product_id")

	return _run_gx_suite(df, "products", add_expectations)


def validate_orders(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
	def add_expectations(v):
		for col in ["order_id", "customer_id", "product_id", "quantity", "unit_price", "total_amount", "order_timestamp"]:
			v.expect_column_values_to_not_be_null(col)
		v.expect_column_values_to_match_regex("order_id", UUID_REGEX)
		v.expect_column_values_to_match_regex("unit_price", r"^\d+(\.\d+)?$")
		v.expect_column_values_to_match_regex("total_amount", r"^\d+(\.\d+)?$")
		v.expect_column_values_to_match_regex("quantity", r"^\d+$")
		v.expect_column_values_to_be_in_set(
			"order_status", ["Pending", "Completed", "Cancelled", "Refunded"]
		)
		v.expect_column_values_to_be_unique("order_id")

	return _run_gx_suite(df, "orders", add_expectations)


def validate_payments(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
	def add_expectations(v):
		for col in ["payment_id", "order_id", "payment_date", "payment_method", "amount", "payment_status"]:
			v.expect_column_values_to_not_be_null(col)
		v.expect_column_values_to_match_regex("payment_id", UUID_REGEX)
		v.expect_column_values_to_match_regex("amount", r"^\d+(\.\d+)?$")
		v.expect_column_values_to_be_in_set("payment_status", ["Successful", "Failed", "Pending"])
		v.expect_column_values_to_be_unique("payment_id")

	return _run_gx_suite(df, "payments", add_expectations)


def validate_inventory(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
	def add_expectations(v):
		for col in ["inventory_id", "product_id", "warehouse_location", "quantity_on_hand", "last_restock_date"]:
			v.expect_column_values_to_not_be_null(col)
		v.expect_column_values_to_match_regex("inventory_id", UUID_REGEX)
		v.expect_column_values_to_match_regex("quantity_on_hand", r"^\d+$")
		v.expect_column_values_to_be_unique("inventory_id")

	return _run_gx_suite(df, "inventory", add_expectations)


def validate_revenue(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
	def add_expectations(v):
		for col in ["revenue_id", "record_date", "total_revenue", "total_profit"]:
			v.expect_column_values_to_not_be_null(col)
		v.expect_column_values_to_match_regex("revenue_id", UUID_REGEX)
		v.expect_column_values_to_match_regex("total_revenue", r"^\d+(\.\d+)?$")
		v.expect_column_values_to_match_regex("total_profit", r"^\d+(\.\d+)?$")
		v.expect_column_values_to_be_unique("revenue_id")

	return _run_gx_suite(df, "revenue", add_expectations)


def validate_returns(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
	def add_expectations(v):
		for col in ["return_id", "order_id", "return_date", "return_reason", "refund_amount", "return_status"]:
			v.expect_column_values_to_not_be_null(col)
		v.expect_column_values_to_match_regex("return_id", UUID_REGEX)
		v.expect_column_values_to_match_regex("refund_amount", r"^\d+(\.\d+)?$")
		v.expect_column_values_to_be_in_set("return_reason", ["Defective", "Wrong Item", "Changed Mind"])
		v.expect_column_values_to_be_in_set("return_status", ["Processed", "Pending", "Rejected"])
		v.expect_column_values_to_be_unique("return_id")

	return _run_gx_suite(df, "returns", add_expectations)


VALIDATORS = {
	"customers": validate_customers,
	"products": validate_products,
	"orders": validate_orders,
	"payments": validate_payments,
	"inventory": validate_inventory,
	"revenue": validate_revenue,
	"returns": validate_returns,
}


def validate(entity: str, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
	"""Validate a DataFrame for the given entity and split valid/invalid rows."""
	fn = VALIDATORS.get(entity)
	if fn is None:
		raise ValueError(f"No validator defined for entity: {entity!r}")
	return fn(df)


__all__ = ["validate"]
