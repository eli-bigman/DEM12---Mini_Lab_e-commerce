"""
main.py

Collector Service Entrypoint.

Orchestrates the generation of all e-commerce data entities
and uploads them to MinIO in time-partitioned paths.

Runs in a continuous loop, generating one micro-batch per interval.
"""

import logging
import os
import time
from datetime import datetime, timezone

from generators import (
    generate_customers,
    generate_inventory,
    generate_orders,
    generate_payments,
    generate_products,
    generate_returns,
    generate_revenue,
)
from uploader import get_minio_client, upload_csv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("collector")


def parse_env() -> dict:
    """Read and validate required environment variables."""
    return {
        "bucket": os.environ.get("MINIO_RAW_BUCKET", "raw-data"),
        "batch_size": int(os.environ.get("COLLECTOR_BATCH_SIZE", "50")),
        "interval": int(os.environ.get("COLLECTOR_BATCH_INTERVAL_SECONDS", "60")),
    }


def extract_ids(csv_content: str, id_col_index: int = 0) -> list[str]:
    """
    Extract a list of IDs from a CSV string by column index.
    Skips the header row.
    """
    ids = []
    lines = csv_content.strip().split("\n")
    for line in lines[1:]:  # skip header
        parts = line.split(",")
        if len(parts) > id_col_index:
            ids.append(parts[id_col_index].strip())
    return ids


def extract_id_amount_pairs(
    csv_content: str,
    id_col_index: int = 0,
    amount_col_index: int = 5,
) -> list[tuple[str, float]]:
    """
    Extract (id, amount) pairs from a CSV string by column indices.
    Skips the header row.
    """
    pairs = []
    lines = csv_content.strip().split("\n")
    for line in lines[1:]:  # skip header
        parts = line.split(",")
        if len(parts) > max(id_col_index, amount_col_index):
            try:
                row_id = parts[id_col_index].strip()
                amount = float(parts[amount_col_index].strip())
                pairs.append((row_id, amount))
            except ValueError:
                continue
    return pairs


def extract_product_prices(csv_content: str) -> list[tuple[str, float]]:
    """
    Extract (product_id, selling_price) pairs from products CSV.
    Column indices: product_id=0, selling_price=4
    """
    return extract_id_amount_pairs(csv_content, id_col_index=0, amount_col_index=4)


def run_batch(client, bucket: str, batch_size: int, now: datetime) -> None:
    """Generate, link, and upload one complete micro-batch of all entities."""
    ts = now.strftime("%Y%m%d_%H%M%S")
    logger.info("Starting batch at %s (size=%d)", ts, batch_size)

    # --- 1. Customers ---
    customers_csv = generate_customers(batch_size=batch_size, reference_dt=now)
    customer_ids = extract_ids(customers_csv, id_col_index=0)
    upload_csv(client, bucket, "customers", f"customers_{ts}.csv", customers_csv, now)

    # --- 2. Products ---
    products_csv = generate_products(batch_size=batch_size)
    product_prices = extract_product_prices(products_csv)
    product_ids = [p[0] for p in product_prices]
    upload_csv(client, bucket, "products", f"products_{ts}.csv", products_csv, now)

    # --- 3. Orders (depends on customers + products) ---
    orders_csv = generate_orders(
        batch_size=batch_size,
        customer_ids=customer_ids,
        product_prices=product_prices,
        reference_dt=now,
    )
    # orders CSV: order_id=0, total_amount=5
    order_amounts = extract_id_amount_pairs(orders_csv, id_col_index=0, amount_col_index=5)
    upload_csv(client, bucket, "orders", f"orders_{ts}.csv", orders_csv, now)

    # --- 4. Payments (depends on orders) ---
    payments_csv = generate_payments(
        batch_size=batch_size,
        order_amounts=order_amounts,
        reference_dt=now,
    )
    upload_csv(client, bucket, "payments", f"payments_{ts}.csv", payments_csv, now)

    # --- 5. Inventory (depends on products) ---
    inventory_csv = generate_inventory(
        batch_size=batch_size,
        product_ids=product_ids,
        reference_dt=now,
    )
    upload_csv(client, bucket, "inventory", f"inventory_{ts}.csv", inventory_csv, now)

    # --- 6. Revenue ---
    # Generate one revenue record per day for the last `batch_size` days
    revenue_csv = generate_revenue(batch_size=min(batch_size, 30), reference_dt=now)
    upload_csv(client, bucket, "revenue", f"revenue_{ts}.csv", revenue_csv, now)

    # --- 7. Returns (depends on orders) ---
    # Returns volume is naturally lower: ~10% of order volume
    returns_batch_size = max(1, batch_size // 10)
    returns_csv = generate_returns(
        batch_size=returns_batch_size,
        order_amounts=order_amounts,
        reference_dt=now,
    )
    upload_csv(client, bucket, "returns", f"returns_{ts}.csv", returns_csv, now)

    logger.info("Batch %s completed successfully.", ts)


def main() -> None:
    config = parse_env()
    logger.info(
        "Collector starting. batch_size=%d, interval=%ds, bucket=%s",
        config["batch_size"],
        config["interval"],
        config["bucket"],
    )

    client = get_minio_client()

    while True:
        now = datetime.now(tz=timezone.utc)
        try:
            run_batch(
                client=client,
                bucket=config["bucket"],
                batch_size=config["batch_size"],
                now=now,
            )
        except Exception as exc:
            logger.error("Batch failed: %s. Will retry in %ds.", exc, config["interval"])

        logger.info("Sleeping for %d seconds...", config["interval"])
        time.sleep(config["interval"])


if __name__ == "__main__":
    main()
