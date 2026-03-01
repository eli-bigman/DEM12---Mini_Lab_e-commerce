"""
generators/returns.py

Generates customer return and refund records.

Fields: return_id, order_id, return_date, return_reason, refund_amount, return_status
"""

import csv
import io
import uuid
from datetime import datetime, timedelta, timezone
from random import choices, randint, uniform


RETURN_REASONS = ["Defective", "Wrong Item", "Changed Mind"]
RETURN_REASON_WEIGHTS = [0.45, 0.30, 0.25]

RETURN_STATUSES = ["Processed", "Pending", "Rejected"]
RETURN_STATUS_WEIGHTS = [0.65, 0.25, 0.10]


def generate_returns(
    batch_size: int,
    order_amounts: list[tuple[str, float]],
    reference_dt: datetime | None = None,
) -> str:
    """
    Generate a batch of return records and return them as a CSV string.

    Parameters
    ----------
    batch_size    : Number of return records to generate.
    order_amounts : List of (order_id, total_amount) tuples to reference (FK).
    reference_dt  : Datetime reference for return_date. Defaults to UTC now.

    Returns
    -------
    CSV-formatted string with header row and `batch_size` data rows.
    """
    if reference_dt is None:
        reference_dt = datetime.now(tz=timezone.utc)

    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow([
        "return_id", "order_id", "return_date",
        "return_reason", "refund_amount", "return_status",
    ])

    for _ in range(batch_size):
        order_id, total_amount = choices(order_amounts)[0]

        # Return happens within 1-30 days after order
        days_offset = randint(1, 30)
        return_date = (reference_dt + timedelta(days=days_offset)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

        # Partial or full refund
        refund_fraction = uniform(0.50, 1.0)
        refund_amount = round(total_amount * refund_fraction, 2)

        writer.writerow([
            str(uuid.uuid4()),
            order_id,
            return_date,
            choices(RETURN_REASONS, weights=RETURN_REASON_WEIGHTS)[0],
            refund_amount,
            choices(RETURN_STATUSES, weights=RETURN_STATUS_WEIGHTS)[0],
        ])

    return output.getvalue()
