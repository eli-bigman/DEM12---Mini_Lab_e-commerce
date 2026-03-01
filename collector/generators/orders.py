"""
generators/orders.py

Generates realistic order records for the e-commerce platform.

Fields: order_id, customer_id, product_id, quantity, unit_price,
        total_amount, order_timestamp, payment_method, order_status
"""

import csv
import io
import uuid
from datetime import datetime, timedelta, timezone
from random import choices, randint, uniform


ORDER_STATUSES = ["Completed", "Cancelled", "Refunded"]
ORDER_STATUS_WEIGHTS = [0.75, 0.15, 0.10]

PAYMENT_METHODS = ["Credit Card", "PayPal", "Stripe", "Bank Transfer", "Crypto"]
PAYMENT_METHOD_WEIGHTS = [0.40, 0.25, 0.20, 0.10, 0.05]


def generate_orders(
    batch_size: int,
    customer_ids: list[str],
    product_prices: list[tuple[str, float]],
    reference_dt: datetime | None = None,
) -> str:
    """
    Generate a batch of order records and return them as a CSV string.

    Parameters
    ----------
    batch_size     : Number of order records to generate.
    customer_ids   : List of customer UUIDs to reference (FK).
    product_prices : List of (product_id, unit_price) tuples to reference (FK).
    reference_dt   : Datetime reference for order_timestamp. Defaults to UTC now.

    Returns
    -------
    CSV-formatted string with header row and `batch_size` data rows.
    """
    if reference_dt is None:
        reference_dt = datetime.now(tz=timezone.utc)

    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow([
        "order_id", "customer_id", "product_id", "quantity", "unit_price",
        "total_amount", "order_timestamp", "payment_method", "order_status",
    ])

    for _ in range(batch_size):
        customer_id = choices(customer_ids)[0]
        product_id, unit_price = choices(product_prices)[0]

        # Variable order sizes: most orders are 1-3 items, occasional bulk
        quantity = choices(
            [1, 2, 3, 4, 5, randint(6, 20)],
            weights=[0.50, 0.25, 0.12, 0.06, 0.04, 0.03],
        )[0]
        total_amount = round(unit_price * quantity, 2)

        # Orders distributed within the last 7 days
        seconds_ago = randint(0, 7 * 24 * 3600)
        order_timestamp = (reference_dt - timedelta(seconds=seconds_ago)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

        writer.writerow([
            str(uuid.uuid4()),
            customer_id,
            product_id,
            quantity,
            unit_price,
            total_amount,
            order_timestamp,
            choices(PAYMENT_METHODS, weights=PAYMENT_METHOD_WEIGHTS)[0],
            choices(ORDER_STATUSES, weights=ORDER_STATUS_WEIGHTS)[0],
        ])

    return output.getvalue()
