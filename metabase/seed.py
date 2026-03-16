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

Required environment variables:
  METABASE_URL              e.g. http://localhost:3000
  METABASE_ADMIN_EMAIL      e.g. admin@example.com
  METABASE_ADMIN_PASSWORD   Must be a strong password (Metabase rejects common ones)
  POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB
  MB_DB_PASS                Password for the metabase_user Postgres role
"""

import os
import time
import logging
from urllib.parse import urlparse

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("metabase_seed")

BASE_URL       = os.environ.get("METABASE_URL", "http://localhost:3000")
ADMIN_EMAIL    = os.environ["METABASE_ADMIN_EMAIL"]
ADMIN_PASSWORD = os.environ["METABASE_ADMIN_PASSWORD"]
PG_HOST        = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT        = int(os.environ.get("POSTGRES_PORT", "5432"))
PG_DB          = os.environ.get("POSTGRES_DB", "ecommerce")
PG_USER        = os.environ.get("POSTGRES_USER", "metabase_user")
PG_PASS        = os.environ["MB_DB_PASS"]


def infer_public_base_url(base_url: str) -> str:
    """Return a browser-friendly Metabase URL (localhost for local Docker setups)."""
    explicit = os.environ.get("METABASE_PUBLIC_URL")
    if explicit:
        return explicit.rstrip("/")

    parsed = urlparse(base_url)
    scheme = parsed.scheme or "http"
    host = parsed.hostname or "localhost"
    if host in {"metabase", "0.0.0.0"}:
        host = "localhost"
    port = f":{parsed.port}" if parsed.port else ""
    return f"{scheme}://{host}{port}"


PUBLIC_BASE_URL = infer_public_base_url(BASE_URL)


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


def setup_metabase() -> str:
    """
    Complete the initial Metabase setup OR log in if already set up.

    Handles three scenarios:
      1. Fresh Metabase (setup-token present, no user) -> run /api/setup
      2. Setup token present but user exists (partial setup) -> fall back to login
      3. No setup token (fully configured) -> login directly
    Returns a session token.
    """
    props = requests.get(f"{BASE_URL}/api/session/properties").json()
    token = props.get("setup-token")

    if not token:
        log.info("Metabase is already set up. Logging in.")
        return login()

    # Attempt initial setup
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

    if r.ok:
        log.info("Metabase setup complete.")
        return r.json()["id"]

    # Setup failed (user already exists, password rejected, etc.) -- fall back to login
    log.warning("Setup endpoint returned %d: %s. Falling back to login.", r.status_code, r.text)
    return login()


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
    if not r.ok:
        log.error("API error %s %s (%d): %s", method.upper(), path, r.status_code, r.text)
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
        "description": "Top-line revenue from completed orders.",
        "query": "SELECT ROUND(total_revenue, 2) AS total_revenue FROM analytics.v_kpi_summary",
        "visualization_settings": {"number_style": "currency"},
    },
    {
        "name": "Total Orders",
        "display": "scalar",
        "description": "Count of unique completed orders.",
        "query": "SELECT total_orders FROM analytics.v_kpi_summary",
        "visualization_settings": {},
    },
    {
        "name": "Average Order Value",
        "display": "scalar",
        "description": "Average revenue generated per completed order.",
        "query": "SELECT avg_order_value FROM analytics.v_kpi_summary",
        "visualization_settings": {"number_style": "currency"},
    },
    {
        "name": "Gross Profit",
        "display": "scalar",
        "description": "Revenue minus product cost across completed orders.",
        "query": "SELECT ROUND(total_profit, 2) AS gross_profit FROM analytics.v_kpi_summary",
        "visualization_settings": {"number_style": "currency"},
    },
    {
        "name": "Customer Growth Rate (%)",
        "display": "scalar",
        "description": "Growth in signup volume for last 30 days vs prior 30 days.",
        "query": "SELECT customer_growth_rate_pct FROM analytics.v_kpi_summary",
        "visualization_settings": {"number_style": "percent"},
    },
    # Chart cards
    {
        "name": "Revenue Trend Over Time",
        "display": "line",
        "description": "Daily trend of completed-order revenue.",
        "query": "SELECT date, daily_revenue FROM analytics.v_revenue_trend_daily ORDER BY date",
        "visualization_settings": {
            "graph.colors": ["#22C55E"],
            "graph.show_values": True,
            "line.interpolate": "monotone",
        },
    },
    {
        "name": "Revenue by Category",
        "display": "bar",
        "description": "Revenue mix by product category.",
        "query": "SELECT category, total_revenue FROM analytics.v_revenue_by_category ORDER BY total_revenue DESC",
        "visualization_settings": {
            "graph.colors": ["#38BDF8", "#34D399", "#FBBF24", "#FB7185", "#A78BFA"],
            "graph.show_values": True,
        },
    },
    {
        "name": "Revenue by Acquisition Channel",
        "display": "bar",
        "description": "Revenue contribution by acquisition channel.",
        "query": "SELECT acquisition_channel, total_revenue FROM analytics.v_revenue_by_channel ORDER BY total_revenue DESC",
        "visualization_settings": {
            "graph.colors": ["#0EA5E9", "#14B8A6", "#F97316", "#A855F7", "#84CC16"],
            "graph.show_values": True,
        },
    },
    {
        "name": "Top 10 Products by Profit",
        "display": "table",
        "description": "Highest-profit products ranked by total profit.",
        "query": "SELECT product_name, category, total_profit, units_sold FROM analytics.v_top_products_profit",
        "visualization_settings": {
            "table.pivot_column": "product_name",
        },
    },
    {
        "name": "Customer Retention Indicators",
        "display": "pie",
        "description": "Order distribution by customer segment.",
        "query": "SELECT customer_segment, total_orders FROM analytics.v_customer_retention",
        "visualization_settings": {
            "graph.colors": ["#22C55E", "#0EA5E9", "#F59E0B", "#EF4444", "#8B5CF6"],
            "pie.show_total": True,
        },
    },
    {
        "name": "Order Status Distribution",
        "display": "pie",
        "description": "Operational order mix across statuses.",
        "query": "SELECT order_status, order_count FROM analytics.v_order_status_distribution",
        "visualization_settings": {
            "graph.colors": ["#22C55E", "#F59E0B", "#EF4444", "#38BDF8", "#A78BFA"],
            "pie.show_total": True,
        },
    },
    {
        "name": "Monthly Revenue vs Profit",
        "display": "line",
        "description": "Monthly top-line revenue versus bottom-line profit.",
        "query": "SELECT DATE_TRUNC('month', date)::date AS month, SUM(daily_revenue) AS monthly_revenue, SUM(daily_profit) AS monthly_profit FROM analytics.v_revenue_trend_daily GROUP BY 1 ORDER BY 1",
        "visualization_settings": {
            "graph.colors": ["#38BDF8", "#22C55E"],
            "graph.show_values": True,
            "line.interpolate": "monotone",
        },
    },
    {
        "name": "Profit Margin by Category (%)",
        "display": "bar",
        "description": "Category ranking by profit margin percentage.",
        "query": "SELECT category, ROUND((total_profit * 100.0) / NULLIF(total_revenue, 0), 2) AS profit_margin_pct FROM analytics.v_revenue_by_category ORDER BY profit_margin_pct DESC",
        "visualization_settings": {
            "graph.colors": ["#10B981", "#34D399", "#6EE7B7", "#A7F3D0"],
            "graph.show_values": True,
        },
    },
    {
        "name": "Orders by Payment Method",
        "display": "bar",
        "description": "Customer payment behavior by method.",
        "query": "SELECT payment_method, COUNT(DISTINCT order_id) AS total_orders, ROUND(SUM(revenue), 2) AS total_revenue FROM analytics.fact_orders WHERE order_status = 'Completed' GROUP BY payment_method ORDER BY total_orders DESC",
        "visualization_settings": {
            "graph.colors": ["#6366F1", "#0EA5E9", "#F59E0B", "#EF4444", "#14B8A6"],
            "graph.show_values": True,
        },
    },
    {
        "name": "Returns by Status",
        "display": "pie",
        "description": "Return case mix and refund exposure by status.",
        "query": "SELECT return_status, COUNT(*) AS return_count, ROUND(SUM(refund_amount), 2) AS total_refund_amount FROM analytics.fact_returns GROUP BY return_status ORDER BY return_count DESC",
        "visualization_settings": {
            "graph.colors": ["#F97316", "#EF4444", "#EAB308", "#38BDF8", "#22C55E"],
            "pie.show_total": True,
        },
    },
]


def create_cards(token: str, db_id: int) -> list[int]:
    """Create or update all dashboard cards and return their IDs."""
    card_ids = []
    existing = {c["name"]: c["id"] for c in api("get", "/api/card", token)}

    for card_def in CARDS:
        payload = {
            "name": card_def["name"],
            "description": card_def.get("description", ""),
            "display": card_def["display"],
            "dataset_query": {
                "type": "native",
                "database": db_id,
                "native": {"query": card_def["query"]},
            },
            "visualization_settings": card_def.get("visualization_settings", {}),
        }

        if card_def["name"] in existing:
            card_id = existing[card_def["name"]]
            api("put", f"/api/card/{card_id}", token, json=payload)
            log.info("Card already exists and was updated: %s (id=%d)", card_def["name"], card_id)
            card_ids.append(card_id)
            continue

        card = api("post", "/api/card", token, json=payload)
        log.info("Created card: %s (id=%d)", card["name"], card["id"])
        card_ids.append(card["id"])

    return card_ids


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

def create_dashboard(token: str, card_ids: list[int]) -> int:
    """Create or update the executive dashboard and make sure all cards are attached."""
    existing = api("get", "/api/dashboard", token)
    dash_id = None
    for dash in existing:
        if dash.get("name") == "E-Commerce Executive Dashboard":
            dash_id = dash["id"]
            log.info("Dashboard already exists (id=%d). Syncing cards ...", dash_id)
            break

    if dash_id is None:
        log.info("Creating executive dashboard ...")
        dash = api("post", "/api/dashboard", token, json={
            "name": "E-Commerce Executive Dashboard",
            "description": "KPI overview and analytical visualisations for e-commerce operations.",
        })
        dash_id = dash["id"]

    dashboard_detail = api("get", f"/api/dashboard/{dash_id}", token)
    existing_dashcards = {
        (dc.get("card", {}) or {}).get("id"): dc.get("id")
        for dc in dashboard_detail.get("dashcards", [])
        if (dc.get("card", {}) or {}).get("id") is not None
    }

    # Add cards to dashboard in a 2-column grid layout
    # Use snake_case field names (card_id) for Metabase API compatibility
    cards_payload = []
    for i, card_id in enumerate(card_ids):
        col = (i % 2) * 12       # 0 or 12  (two columns of width 12)
        row = (i // 2) * 8       # stack rows of height 8
        cards_payload.append({
            "id": existing_dashcards.get(card_id, -(i + 1)),
            "card_id": card_id,
            "col": col,
            "row": row,
            "size_x": 12,
            "size_y": 8,
        })

    api("put", f"/api/dashboard/{dash_id}", token, json={"dashcards": cards_payload})
    log.info("Dashboard synced with %d cards (id=%d).", len(card_ids), dash_id)
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
        PUBLIC_BASE_URL, dash_id,
    )


if __name__ == "__main__":
    main()
