"""
generators/customers.py

Generates realistic customer records for the e-commerce platform.

Fields: customer_id, signup_date, country, acquisition_channel, customer_segment
"""

import csv
import io
import uuid
from datetime import datetime, timedelta, timezone
from random import choices, randint

COUNTRIES = [
    "US", "UK", "DE", "FR", "CA", "AU", "NG", "GH", "IN", "BR",
    "JP", "ZA", "MX", "NL", "SE", "SG", "KE", "PL", "IT", "ES",
]

ACQUISITION_CHANNELS = ["Organic", "Facebook", "Google", "Email"]
ACQUISITION_WEIGHTS = [0.35, 0.25, 0.25, 0.15]

CUSTOMER_SEGMENTS = ["New", "Returning", "VIP"]
SEGMENT_WEIGHTS = [0.55, 0.35, 0.10]


def generate_customers(batch_size: int, reference_dt: datetime | None = None) -> str:
    """
    Generate a batch of customer records and return them as a CSV string.

    Parameters
    ----------
    batch_size   : Number of customer records to generate.
    reference_dt : Datetime reference for signup_date range. Defaults to UTC now.

    Returns
    -------
    CSV-formatted string with header row and `batch_size` data rows.
    """
    if reference_dt is None:
        reference_dt = datetime.now(tz=timezone.utc)

    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(
        ["customer_id", "signup_date", "country", "acquisition_channel", "customer_segment"]
    )

    for _ in range(batch_size):
        days_ago = randint(0, 730)  # signed up within the last 2 years
        signup_date = (reference_dt - timedelta(days=days_ago)).date().isoformat()

        writer.writerow([
            str(uuid.uuid4()),
            signup_date,
            choices(COUNTRIES)[0],
            choices(ACQUISITION_CHANNELS, weights=ACQUISITION_WEIGHTS)[0],
            choices(CUSTOMER_SEGMENTS, weights=SEGMENT_WEIGHTS)[0],
        ])

    return output.getvalue()
