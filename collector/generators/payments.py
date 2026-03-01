"""
generators/payments.py

Generates realistic payment records linked to orders.

Fields: payment_id, order_id, payment_date, payment_method, amount, payment_status
"""

import csv
import io
import uuid
from datetime import datetime, timedelta, timezone
from random import choices, randint


PAYMENT_METHODS = ["Credit Card", "PayPal", "Stripe"]
PAYMENT_METHOD_WEIGHTS = [0.50, 0.30, 0.20]

PAYMENT_STATUSES = ["Successful", "Failed", "Pending"]
PAYMENT_STATUS_WEIGHTS = [0.85, 0.10, 0.05]


def generate_payments(
    batch_size: int,
    order_amounts: list[tuple[str, float]],
    reference_dt: datetime | None = None,
) -> str:
    """
    Generate a batch of payment records and return them as a CSV string.

    Parameters
    ----------
    batch_size    : Number of payment records to generate.
    order_amounts : List of (order_id, amount) tuples to reference (FK).
    reference_dt  : Datetime reference for payment_date. Defaults to UTC now.

    Returns
    -------
    CSV-formatted string with header row and `batch_size` data rows.
    """
    if reference_dt is None:
        reference_dt = datetime.now(tz=timezone.utc)

    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow([
        "payment_id", "order_id", "payment_date",
        "payment_method", "amount", "payment_status",
    ])

    for _ in range(batch_size):
        order_id, amount = choices(order_amounts)[0]

        # Payment happens within 0-2 days after order
        seconds_offset = randint(0, 2 * 24 * 3600)
        payment_date = (reference_dt + timedelta(seconds=seconds_offset)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

        writer.writerow([
            str(uuid.uuid4()),
            order_id,
            payment_date,
            choices(PAYMENT_METHODS, weights=PAYMENT_METHOD_WEIGHTS)[0],
            amount,
            choices(PAYMENT_STATUSES, weights=PAYMENT_STATUS_WEIGHTS)[0],
        ])

    return output.getvalue()
