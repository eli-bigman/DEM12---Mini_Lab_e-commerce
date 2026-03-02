"""
metabase/seed.py

Automates Metabase initial configuration via the Metabase REST API.

What this script does:
  1. Waits for Metabase to be ready (polls /api/health).
  2. Completes the initial setup (creates admin user) if not already done.
  3. Logs in and retrieves a session token.
  4. Adds the ecommerce PostgreSQL database as a Metabase data source.
  5. Creates SQL-based question cards for each KPI and chart.
  6. Creates the executive dashboard and attaches all cards.

Usage:
  python seed.py

Environment variables (read from .env or set in shell):
  METABASE_URL            e.g. http://localhost:3000
  METABASE_ADMIN_EMAIL    e.g. admin@example.com
  METABASE_ADMIN_PASSWORD e.g. admin123
  POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB,
  POSTGRES_USER, POSTGRES_PASSWORD
"""

import os
import sys
import time
import logging

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("metabase_seed")

BASE_URL          = os.environ.get("METABASE_URL", "http://localhost:3000")
ADMIN_EMAIL       = os.environ.get("METABASE_ADMIN_EMAIL", "admin@example.com")
ADMIN_PASSWORD    = os.environ.get("METABASE_ADMIN_PASSWORD", "admin123")
PG_HOST           = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT           = int(os.environ.get("POSTGRES_PORT", "5432"))
PG_DB             = os.environ.get("POSTGRES_DB", "ecommerce")
PG_USER           = os.environ.get("POSTGRES_USER", "metabase_user")
PG_PASS           = os.environ.get("MB_DB_PASS", "metabase_secret_password")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def wait_for_metabase(timeout: int = 180) -> None:
    """Poll /api/health until Metabase responds as healthy."""
    log.info("Waiting for Metabase to be ready at %s ...", BASE_URL)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{BASE_URL}/api/health", timeout=5)
            if r.status_code == 200 and r.json().get("status") == "ok":
                log.info("Metabase is ready.")
                return
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(5)
    raise TimeoutError("Metabase did not become ready within the timeout period.")


def needs_setup() -> bool:
    """Return True if Metabase has not been set up yet."""
    r = requests.get(f"{BASE_URL}/api/session/properties")
    return r.json().get("setup-token") is not None


def setup_metabase() -> str:
    """Complete the initial Metabase setup. Returns a session token."""
    props = requests.get(f"{BASE_URL}/api/session/properties").json()
    token = props.get("setup-token")
    if not token:
        log.info("Metabase is already set up. Logging in.")
        return login()

    log.info("Running initial Metabase setup ...")
    payload = {
        "token": token,
        "user": {
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD,
            "first_name": "Admin",
            "last_name": "User",
            "site_name": "E-Commerce Platform",
        },
        "prefs": {"site_name": "E-Commerce Platform", "allow_tracking": False},
    }
    r = requests.post(f"{BASE_URL}/api/setup", json=payload)
    r.raise_for_status()
    log.info("Metabase setup complete.")
    return r.json()["id"]


def login() -> str:
    """Authenticate and return a Metabase session token."""
    r = requests.post(
        f"{BASE_URL}/api/session",
        json={"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD},
    )
    r.raise_for_status()
    return r.json()["id"]


def api(method: str, path: str, token: str, **kwargs) -> dict:
    """Authenticated Metabase API call."""
    r = getattr(requests, method)(
        f"{BASE_URL}{path}",
        headers={"X-Metabase-Session": token},
        **kwargs,
    )
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------

def add_database(token: str) -> int:
    """Add the ecommerce PostgreSQL database. Returns the database ID."""
    # Check if already exists
    databases = api("get", "/api/database", token)
    for db in databases.get("data", []):
        if db.get("name") == "E-Commerce Analytics":
            log.info("Database already exists (id=%d).", db["id"])
            return db["id"]

    log.info("Adding PostgreSQL database connection ...")
    payload = {
        "engine": "postgres",
        "name": "E-Commerce Analytics",
        "details": {
            "host": PG_HOST,
            "port": PG_PORT,
            "dbname": PG_DB,
            "user": PG_USER,
            "password": PG_PASS,
            "schema-filters-type": "inclusion",
            "schema-filters-patterns": "analytics",
        },
        "auto_run_queries": True,
        "is_full_sync": True,
    }
    db = api("post", "/api/database", token, json=payload)
    log.info("Database added (id=%d). Triggering sync ...", db["id"])
    api("post", f"/api/database/{db['id']}/sync_schema", token)
    time.sleep(10)  # allow sync to complete
    return db["id"]


# ---------------------------------------------------------------------------
# Question / Card definitions
# ---------------------------------------------------------------------------

CARDS = [
    # KPI cards (scalar metrics)
    {
        "name": "Total Revenue",
        "display": "scalar",
        "query": "SELECT ROUND(total_revenue, 2) AS total_revenue FROM analytics.v_kpi_summary",
    },
    {
        "name": "Total Orders",
        "display": "scalar",
        "query": "SELECT total_orders FROM analytics.v_kpi_summary",
    },
    {
        "name": "Average Order Value",
        "display": "scalar",
        "query": "SELECT avg_order_value FROM analytics.v_kpi_summary",
    },
    {
        "name": "Gross Profit",
        "display": "scalar",
        "query": "SELECT ROUND(total_profit, 2) AS gross_profit FROM analytics.v_kpi_summary",
    },
    {
        "name": "Customer Growth Rate (%)",
        "display": "scalar",
        "query": "SELECT customer_growth_rate_pct FROM analytics.v_kpi_summary",
    },
    # Chart cards
    {
        "name": "Revenue Trend Over Time",
        "display": "line",
        "query": "SELECT date, daily_revenue FROM analytics.v_revenue_trend_daily ORDER BY date",
    },
    {
        "name": "Revenue by Category",
        "display": "bar",
        "query": "SELECT category, total_revenue FROM analytics.v_revenue_by_category ORDER BY total_revenue DESC",
    },
    {
        "name": "Revenue by Acquisition Channel",
        "display": "bar",
        "query": "SELECT acquisition_channel, total_revenue FROM analytics.v_revenue_by_channel ORDER BY total_revenue DESC",
    },
    {
        "name": "Top 10 Products by Profit",
        "display": "table",
        "query": "SELECT product_name, category, total_profit, units_sold FROM analytics.v_top_products_profit",
    },
    {
        "name": "Customer Retention Indicators",
        "display": "pie",
        "query": "SELECT customer_segment, total_orders FROM analytics.v_customer_retention",
    },
    {
        "name": "Order Status Distribution",
        "display": "pie",
        "query": "SELECT order_status, order_count FROM analytics.v_order_status_distribution",
    },
]


def create_cards(token: str, db_id: int) -> list[int]:
    """Create all dashboard cards and return their IDs."""
    card_ids = []
    existing = {c["name"]: c["id"] for c in api("get", "/api/card", token)}

    for card_def in CARDS:
        if card_def["name"] in existing:
            log.info("Card already exists: %s", card_def["name"])
            card_ids.append(existing[card_def["name"]])
            continue

        payload = {
            "name": card_def["name"],
            "display": card_def["display"],
            "dataset_query": {
                "type": "native",
                "database": db_id,
                "native": {"query": card_def["query"]},
            },
            "visualization_settings": {},
        }
        card = api("post", "/api/card", token, json=payload)
        log.info("Created card: %s (id=%d)", card["name"], card["id"])
        card_ids.append(card["id"])

    return card_ids


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

def create_dashboard(token: str, card_ids: list[int]) -> int:
    """Create the executive dashboard and add all cards to it."""
    existing = api("get", "/api/dashboard", token)
    for dash in existing:
        if dash.get("name") == "E-Commerce Executive Dashboard":
            log.info("Dashboard already exists (id=%d).", dash["id"])
            return dash["id"]

    log.info("Creating executive dashboard ...")
    dash = api("post", "/api/dashboard", token, json={
        "name": "E-Commerce Executive Dashboard",
        "description": "KPI overview and analytical visualisations for e-commerce operations.",
    })
    dash_id = dash["id"]

    # Add cards to dashboard in a 2-column grid layout
    cards_payload = []
    for i, card_id in enumerate(card_ids):
        col = (i % 2) * 12       # 0 or 12  (two columns of width 12)
        row = (i // 2) * 8       # stack rows of height 8
        cards_payload.append({
            "cardId": card_id,
            "col": col,
            "row": row,
            "size_x": 12,
            "size_y": 8,
        })

    api("put", f"/api/dashboard/{dash_id}", token, json={"dashcards": cards_payload})
    log.info("Dashboard created with %d cards (id=%d).", len(card_ids), dash_id)
    return dash_id


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    wait_for_metabase()
    token = setup_metabase()
    db_id = add_database(token)
    card_ids = create_cards(token, db_id)
    dash_id = create_dashboard(token, card_ids)
    log.info(
        "Seeding complete. Open your dashboard at %s/dashboard/%d",
        BASE_URL, dash_id,
    )


if __name__ == "__main__":
    main()
