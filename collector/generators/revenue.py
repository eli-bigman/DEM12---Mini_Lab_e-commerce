"""
generators/revenue.py

Generates daily aggregated revenue snapshot records.

Fields: revenue_id, record_date, total_revenue, total_profit
"""

import csv
import io
import uuid
from datetime import datetime, timedelta, timezone
from random import randint, uniform


DAILY_REVENUE_RANGE = (5_000.0, 150_000.0)
PROFIT_MARGIN_RANGE = (0.15, 0.40)


def generate_revenue(
    batch_size: int,
    reference_dt: datetime | None = None,
) -> str:
    """
    Generate a batch of daily aggregated revenue records and return them as a CSV string.

    Parameters
    ----------
    batch_size   : Number of revenue records to generate (each represents one day).
    reference_dt : Datetime reference for record_date range. Defaults to UTC now.

    Returns
    -------
    CSV-formatted string with header row and `batch_size` data rows.
    """
    if reference_dt is None:
        reference_dt = datetime.now(tz=timezone.utc)

    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["revenue_id", "record_date", "total_revenue", "total_profit"])

    for i in range(batch_size):
        # Each record represents a distinct past day
        record_date = (reference_dt - timedelta(days=i)).date().isoformat()
        total_revenue = round(uniform(*DAILY_REVENUE_RANGE), 2)
        margin = uniform(*PROFIT_MARGIN_RANGE)
        total_profit = round(total_revenue * margin, 2)

        writer.writerow([
            str(uuid.uuid4()),
            record_date,
            total_revenue,
            total_profit,
        ])

    return output.getvalue()
