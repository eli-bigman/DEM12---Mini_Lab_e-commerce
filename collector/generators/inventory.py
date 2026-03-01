"""
generators/inventory.py

Generates inventory records representing warehouse stock levels.

Fields: inventory_id, product_id, warehouse_location, quantity_on_hand, last_restock_date
"""

import csv
import io
import uuid
from datetime import datetime, timedelta, timezone
from random import choices, randint

WAREHOUSE_LOCATIONS = [
    "New York, US",
    "Los Angeles, US",
    "London, UK",
    "Berlin, DE",
    "Lagos, NG",
    "Accra, GH",
    "Singapore, SG",
    "Sydney, AU",
    "Toronto, CA",
    "Dubai, AE",
]


def generate_inventory(
    batch_size: int,
    product_ids: list[str],
    reference_dt: datetime | None = None,
) -> str:
    """
    Generate a batch of inventory records and return them as a CSV string.

    Parameters
    ----------
    batch_size   : Number of inventory records to generate.
    product_ids  : List of product UUIDs to reference (FK).
    reference_dt : Datetime reference for last_restock_date. Defaults to UTC now.

    Returns
    -------
    CSV-formatted string with header row and `batch_size` data rows.
    """
    if reference_dt is None:
        reference_dt = datetime.now(tz=timezone.utc)

    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow([
        "inventory_id", "product_id", "warehouse_location",
        "quantity_on_hand", "last_restock_date",
    ])

    for _ in range(batch_size):
        days_ago = randint(0, 90)
        last_restock_date = (reference_dt - timedelta(days=days_ago)).date().isoformat()

        writer.writerow([
            str(uuid.uuid4()),
            choices(product_ids)[0],
            choices(WAREHOUSE_LOCATIONS)[0],
            randint(0, 5000),
            last_restock_date,
        ])

    return output.getvalue()
