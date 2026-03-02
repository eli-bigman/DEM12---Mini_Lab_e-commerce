"""
tests/conftest.py

Shared pytest fixtures providing sample pandas DataFrames for each entity.
These represent clean, valid rows that should pass all validators and cleaners.
"""

import pandas as pd
import pytest


@pytest.fixture
def valid_customers_df():
    return pd.DataFrame([
        {
            "customer_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "signup_date": "2024-01-15",
            "country": "US",
            "acquisition_channel": "Organic",
            "customer_segment": "New",
        },
        {
            "customer_id": "b2c3d4e5-f6a7-8901-bcde-f12345678901",
            "signup_date": "2024-03-22",
            "country": "UK",
            "acquisition_channel": "Google",
            "customer_segment": "VIP",
        },
    ])


@pytest.fixture
def valid_products_df():
    return pd.DataFrame([
        {
            "product_id": "c3d4e5f6-a7b8-9012-cdef-123456789012",
            "product_name": "Wireless Headphones",
            "category": "Electronics",
            "cost_price": "45.00",
            "selling_price": "89.99",
            "inventory_quantity": "200",
        },
    ])


@pytest.fixture
def valid_orders_df():
    return pd.DataFrame([
        {
            "order_id": "d4e5f6a7-b8c9-0123-defa-234567890123",
            "customer_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "product_id": "c3d4e5f6-a7b8-9012-cdef-123456789012",
            "quantity": "2",
            "unit_price": "89.99",
            "total_amount": "179.98",
            "order_timestamp": "2024-06-15T14:23:00Z",
            "payment_method": "Credit Card",
            "order_status": "Completed",
        },
    ])


@pytest.fixture
def valid_payments_df():
    return pd.DataFrame([
        {
            "payment_id": "e5f6a7b8-c9d0-1234-efab-345678901234",
            "order_id": "d4e5f6a7-b8c9-0123-defa-234567890123",
            "payment_date": "2024-06-15T14:25:00Z",
            "payment_method": "Credit Card",
            "amount": "179.98",
            "payment_status": "Successful",
        },
    ])


@pytest.fixture
def valid_inventory_df():
    return pd.DataFrame([
        {
            "inventory_id": "f6a7b8c9-d0e1-2345-fabc-456789012345",
            "product_id": "c3d4e5f6-a7b8-9012-cdef-123456789012",
            "warehouse_location": "New York, US",
            "quantity_on_hand": "500",
            "last_restock_date": "2024-05-01",
        },
    ])


@pytest.fixture
def valid_revenue_df():
    return pd.DataFrame([
        {
            "revenue_id": "a7b8c9d0-e1f2-3456-abcd-567890123456",
            "record_date": "2024-06-15",
            "total_revenue": "12500.00",
            "total_profit": "4200.00",
        },
    ])


@pytest.fixture
def valid_returns_df():
    return pd.DataFrame([
        {
            "return_id": "b8c9d0e1-f2a3-4567-bcde-678901234567",
            "order_id": "d4e5f6a7-b8c9-0123-defa-234567890123",
            "return_date": "2024-06-20T10:00:00Z",
            "return_reason": "Defective",
            "refund_amount": "89.99",
            "return_status": "Processed",
        },
    ])
