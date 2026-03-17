"""
metabase/dashboard.py

Automates Metabase initial configuration via the Metabase REST API.

What this script does:
  1. Waits for Metabase to be ready (polls /api/health).
  2. Completes the initial setup (creates admin user) if not already done.
  3. Logs in and retrieves a session token.
  4. Adds the ecommerce PostgreSQL database as a Metabase data source.
  5. Creates SQL-based question cards for each KPI and chart.
  6. Creates the executive dashboard and attaches all cards.

Usage:
    python dashboard.py

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
log = logging.getLogger("metabase_dashboard")

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
    {
        "name": "Revenue per Quarter (QoQ)",
        "display": "trend",
        "description": "Quarterly revenue with automatic previous-quarter comparison.",
        "query": "SELECT DATE_TRUNC('quarter', order_timestamp)::date AS quarter_start, ROUND(SUM(revenue), 2) AS revenue FROM analytics.fact_orders WHERE order_status = 'Completed' [[AND {{date_range}}]] GROUP BY 1 ORDER BY 1",
        "visualization_settings": {
            "graph.colors": ["#22C55E"],
        },
    },
    {
        "name": "Orders per Quarter (QoQ)",
        "display": "trend",
        "description": "Quarterly order count with previous-quarter comparison.",
        "query": "SELECT DATE_TRUNC('quarter', order_timestamp)::date AS quarter_start, COUNT(DISTINCT order_id) AS total_orders FROM analytics.fact_orders WHERE order_status = 'Completed' [[AND {{date_range}}]] GROUP BY 1 ORDER BY 1",
        "visualization_settings": {
            "graph.colors": ["#3B82F6"],
        },
    },
    {
        "name": "Gross Profit per Quarter (QoQ)",
        "display": "trend",
        "description": "Quarterly gross profit with previous-quarter comparison.",
        "query": "SELECT DATE_TRUNC('quarter', order_timestamp)::date AS quarter_start, ROUND(SUM(profit), 2) AS gross_profit FROM analytics.fact_orders WHERE order_status = 'Completed' [[AND {{date_range}}]] GROUP BY 1 ORDER BY 1",
        "visualization_settings": {
            "graph.colors": ["#10B981"],
        },
    },
    # Chart cards
    {
        "name": "Revenue Trend Over Time",
        "display": "line",
        "description": "Daily trend of completed-order revenue.",
        "query": "SELECT date, daily_revenue FROM analytics.v_revenue_trend_daily [[WHERE {{date_range}}]] ORDER BY date",
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
        "query": "SELECT DATE_TRUNC('month', date)::date AS month, SUM(daily_revenue) AS monthly_revenue, SUM(daily_profit) AS monthly_profit FROM analytics.v_revenue_trend_daily [[WHERE {{date_range}}]] GROUP BY 1 ORDER BY 1",
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
        "query": "SELECT payment_method, COUNT(DISTINCT order_id) AS total_orders, ROUND(SUM(revenue), 2) AS total_revenue FROM analytics.fact_orders WHERE order_status = 'Completed' [[AND {{date_range}}]] GROUP BY payment_method ORDER BY total_orders DESC",
        "visualization_settings": {
            "graph.colors": ["#6366F1", "#0EA5E9", "#F59E0B", "#EF4444", "#14B8A6"],
            "graph.show_values": True,
        },
    },
    {
        "name": "Returns by Status",
        "display": "pie",
        "description": "Return case mix by status.",
        "query": "SELECT return_status, COUNT(*) AS return_count FROM analytics.fact_returns [[WHERE {{date_range}}]] GROUP BY return_status ORDER BY return_count DESC",
        "visualization_settings": {
            "graph.colors": ["#F97316", "#EF4444", "#EAB308", "#38BDF8", "#22C55E"],
            "pie.show_total": True,
        },
    },
    {
        "name": "Monthly Active Customers",
        "display": "line",
        "description": "Unique purchasing customers by month.",
        "query": "SELECT DATE_TRUNC('month', order_timestamp)::date AS month, COUNT(DISTINCT customer_sk) AS active_customers FROM analytics.fact_orders WHERE order_status = 'Completed' [[AND {{date_range}}]] GROUP BY 1 ORDER BY 1",
        "visualization_settings": {
            "graph.colors": ["#2563EB"],
            "graph.show_values": True,
            "line.interpolate": "monotone",
        },
    },
    {
        "name": "Orders and Revenue by Weekday",
        "display": "bar",
        "description": "Weekly purchasing behavior split by weekday.",
        "query": "SELECT TRIM(TO_CHAR(order_timestamp, 'Dy')) AS weekday, EXTRACT(DOW FROM order_timestamp) AS weekday_order, COUNT(DISTINCT order_id) AS total_orders, ROUND(SUM(revenue), 2) AS total_revenue FROM analytics.fact_orders WHERE order_status = 'Completed' [[AND {{date_range}}]] GROUP BY 1, 2 ORDER BY 2",
        "visualization_settings": {
            "graph.colors": ["#0EA5E9", "#22C55E"],
            "graph.show_values": True,
        },
    },
    {
        "name": "Top 10 Countries by Revenue",
        "display": "bar",
        "description": "Geographic concentration of revenue and order volume.",
        "query": "SELECT COALESCE(dc.country, 'Unknown') AS country, ROUND(SUM(fo.revenue), 2) AS total_revenue, COUNT(DISTINCT fo.order_id) AS total_orders FROM analytics.fact_orders fo JOIN analytics.dim_customers dc ON dc.customer_sk = fo.customer_sk WHERE fo.order_status = 'Completed' [[AND {{date_range}}]] GROUP BY 1 ORDER BY total_revenue DESC LIMIT 10",
        "visualization_settings": {
            "graph.colors": ["#14B8A6", "#3B82F6", "#F59E0B", "#EF4444", "#A855F7"],
            "graph.show_values": True,
        },
    },
    {
        "name": "Monthly Refund Trend",
        "display": "line",
        "description": "Refund amount trend over time to monitor return pressure.",
        "query": "SELECT DATE_TRUNC('month', return_date)::date AS month, ROUND(SUM(refund_amount), 2) AS total_refund_amount FROM analytics.fact_returns [[WHERE {{date_range}}]] GROUP BY 1 ORDER BY 1",
        "visualization_settings": {
            "graph.colors": ["#F97316"],
            "graph.show_values": True,
            "line.interpolate": "monotone",
        },
    },
    {
        "name": "Repeat Customer Rate (%)",
        "display": "scalar",
        "description": "Share of customers with more than one completed order.",
        "query": "WITH customer_orders AS (SELECT customer_sk, COUNT(DISTINCT order_id) AS order_count FROM analytics.fact_orders WHERE order_status = 'Completed' [[AND {{date_range}}]] GROUP BY customer_sk), totals AS (SELECT COUNT(*)::NUMERIC AS all_customers, COUNT(*) FILTER (WHERE order_count > 1)::NUMERIC AS repeat_customers FROM customer_orders) SELECT ROUND((repeat_customers * 100.0) / NULLIF(all_customers, 0), 2) AS repeat_customer_rate_pct FROM totals",
        "visualization_settings": {
            "number_style": "percent",
        },
    },
    {
        "name": "Average Items per Order",
        "display": "scalar",
        "description": "Average quantity of items sold per completed order.",
        "query": "SELECT ROUND(SUM(quantity)::NUMERIC / NULLIF(COUNT(DISTINCT order_id), 0), 2) AS avg_items_per_order FROM analytics.fact_orders WHERE order_status = 'Completed' [[AND {{date_range}}]]",
        "visualization_settings": {},
    },
]

CARD_DATE_FIELDS = {
    "Revenue per Quarter (QoQ)": ("analytics", "fact_orders", "order_timestamp"),
    "Orders per Quarter (QoQ)": ("analytics", "fact_orders", "order_timestamp"),
    "Gross Profit per Quarter (QoQ)": ("analytics", "fact_orders", "order_timestamp"),
    "Revenue Trend Over Time": ("analytics", "v_revenue_trend_daily", "date"),
    "Monthly Revenue vs Profit": ("analytics", "v_revenue_trend_daily", "date"),
    "Orders by Payment Method": ("analytics", "fact_orders", "order_timestamp"),
    "Returns by Status": ("analytics", "fact_returns", "return_date"),
    "Monthly Active Customers": ("analytics", "fact_orders", "order_timestamp"),
    "Orders and Revenue by Weekday": ("analytics", "fact_orders", "order_timestamp"),
    "Top 10 Countries by Revenue": ("analytics", "fact_orders", "order_timestamp"),
    "Monthly Refund Trend": ("analytics", "fact_returns", "return_date"),
    "Repeat Customer Rate (%)": ("analytics", "fact_orders", "order_timestamp"),
    "Average Items per Order": ("analytics", "fact_orders", "order_timestamp"),
}

SECTIONS = [
    {
        "key": "health",
        "title": "Overall Business Health",
        "summary": "Business KPIs look strong based on the consistent MoM revenue growth following the growth in the number of orders.",
    },
    {
        "key": "deep_dive",
        "title": "Deeper Dive",
        "summary": "Leading products by revenue and order contribution across key channels and geographies.",
    },
    {
        "key": "ops_risk",
        "title": "Operations & Risk",
        "summary": "Returns, fulfillment status, and customer retention trends remain stable with manageable refund pressure.",
    },
]

DASHBOARD_PARAMETERS = [
    {
        "id": "date_range",
        "name": "Date Range",
        "slug": "date_range",
        "type": "date/all-options",
        "sectionId": "date",
    },
    {
        "id": "date_grouping",
        "name": "Date Grouping",
        "slug": "date_grouping",
        "type": "temporal-unit",
        "sectionId": "temporal-unit",
    },
    {
        "id": "product_category",
        "name": "Product Category",
        "slug": "product_category",
        "type": "category",
        "sectionId": "id",
    },
    {
        "id": "vendor",
        "name": "Vendor",
        "slug": "vendor",
        "type": "category",
        "sectionId": "id",
    },
]

CARD_SECTIONS = {
    "Total Revenue": "health",
    "Total Orders": "health",
    "Average Order Value": "health",
    "Gross Profit": "health",
    "Customer Growth Rate (%)": "health",
    "Revenue per Quarter (QoQ)": "health",
    "Orders per Quarter (QoQ)": "health",
    "Gross Profit per Quarter (QoQ)": "health",
    "Revenue Trend Over Time": "deep_dive",
    "Monthly Revenue vs Profit": "deep_dive",
    "Revenue by Category": "deep_dive",
    "Profit Margin by Category (%)": "deep_dive",
    "Revenue by Acquisition Channel": "deep_dive",
    "Top 10 Countries by Revenue": "deep_dive",
    "Monthly Active Customers": "deep_dive",
    "Top 10 Products by Profit": "deep_dive",
    "Orders by Payment Method": "deep_dive",
    "Orders and Revenue by Weekday": "deep_dive",
    "Customer Retention Indicators": "ops_risk",
    "Repeat Customer Rate (%)": "ops_risk",
    "Average Items per Order": "ops_risk",
    "Order Status Distribution": "ops_risk",
    "Returns by Status": "ops_risk",
    "Monthly Refund Trend": "ops_risk",
}


def create_cards(token: str, db_id: int) -> list[tuple[str, int]]:
    """Create or update all dashboard cards and return (card_name, card_id) pairs."""

    def build_field_lookup() -> dict[tuple[str, str, str], int]:
        metadata = api("get", f"/api/database/{db_id}/metadata", token)
        field_lookup = {}
        for table in metadata.get("tables", []):
            schema = table.get("schema")
            table_name = table.get("name")
            for field in table.get("fields", []):
                field_name = field.get("name")
                field_id = field.get("id")
                if schema and table_name and field_name and field_id is not None:
                    field_lookup[(schema, table_name, field_name)] = field_id
        return field_lookup

    field_lookup = build_field_lookup()

    cards = []
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

        date_field_key = CARD_DATE_FIELDS.get(card_def["name"])
        if date_field_key and "{{date_range}}" in card_def["query"]:
            field_id = field_lookup.get(date_field_key)
            if field_id is None:
                log.warning("Date filter field not found for card '%s': %s", card_def["name"], date_field_key)
            else:
                payload["dataset_query"]["native"]["template-tags"] = {
                    "date_range": {
                        "id": "date_range",
                        "name": "date_range",
                        "display-name": "Date Range",
                        "type": "dimension",
                        "dimension": ["field", field_id, None],
                        "widget-type": "date/all-options",
                    },
                }

        if card_def["name"] in existing:
            card_id = existing[card_def["name"]]
            api("put", f"/api/card/{card_id}", token, json=payload)
            log.info("Card already exists and was updated: %s (id=%d)", card_def["name"], card_id)
            cards.append((card_def["name"], card_id))
            continue

        card = api("post", "/api/card", token, json=payload)
        log.info("Created card: %s (id=%d)", card["name"], card["id"])
        cards.append((card_def["name"], card["id"]))

    return cards


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

def create_dashboard(token: str, cards: list[tuple[str, int]]) -> int:
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
            "width": "fixed",
        })
        dash_id = dash["id"]

    dashboard_detail = api("get", f"/api/dashboard/{dash_id}", token)
    existing_dashcards = {
        (dc.get("card", {}) or {}).get("id"): dc.get("id")
        for dc in dashboard_detail.get("dashcards", [])
        if (dc.get("card", {}) or {}).get("id") is not None
    }

    existing_tabs = {tab.get("name", "").strip(): tab for tab in dashboard_detail.get("tabs", [])}
    tabs_payload = []
    tab_id_by_section = {}
    for i, section in enumerate(SECTIONS, start=1):
        existing_tab = existing_tabs.get(section["title"])
        tab_id = existing_tab["id"] if existing_tab else -i
        tab_id_by_section[section["key"]] = tab_id
        tabs_payload.append({"id": tab_id, "name": section["title"]})

    existing_text_dashcards = {}
    for dashcard in dashboard_detail.get("dashcards", []):
        if dashcard.get("card_id") is not None:
            continue
        tab_id = dashcard.get("dashboard_tab_id")
        text_value = (dashcard.get("visualization_settings", {}) or {}).get("text", "")
        for section in SECTIONS:
            if tab_id == tab_id_by_section[section["key"]] and section["title"] in text_value:
                existing_text_dashcards[section["key"]] = dashcard.get("id")
                break

    card_defs = {card["name"]: card for card in CARDS}
    section_positions = {section["key"]: 0 for section in SECTIONS}
    header_height = 3

    # Add one markdown-style text card per tab as a local title/subtitle header.
    cards_payload = []
    next_virtual_id = -(len(cards) + len(SECTIONS) + 1)
    for section in SECTIONS:
        section_key = section["key"]
        header_text = f"## {section['title']}\n{section['summary']}"
        cards_payload.append({
            "id": existing_text_dashcards.get(section_key, next_virtual_id),
            "card_id": None,
            "dashboard_tab_id": tab_id_by_section[section_key],
            "col": 0,
            "row": 0,
            "size_x": 24,
            "size_y": header_height,
            "visualization_settings": {
                "virtual_card": {
                    "name": None,
                    "display": "text",
                    "dataset_query": {},
                    "visualization_settings": {},
                    "archived": False,
                },
                "text": header_text,
            },
        })
        next_virtual_id -= 1

    # Add query cards to dashboard with per-section layout and tab assignment.
    for idx_global, (card_name, card_id) in enumerate(cards):
        section_key = CARD_SECTIONS.get(card_name, "deep_dive")
        idx = section_positions[section_key]
        section_positions[section_key] += 1

        card_display = card_defs.get(card_name, {}).get("display")
        if card_display in {"scalar", "trend"}:
            size_x = 6
            size_y = 6
            col = (idx % 4) * 6
            row = header_height + ((idx // 4) * 6)
        else:
            size_x = 12
            size_y = 8
            col = (idx % 2) * 12
            row = header_height + ((idx // 2) * 8)

        cards_payload.append({
            "id": existing_dashcards.get(card_id, -(idx_global + 1)),
            "card_id": card_id,
            "dashboard_tab_id": tab_id_by_section[section_key],
            "col": col,
            "row": row,
            "size_x": size_x,
            "size_y": size_y,
            "parameter_mappings": (
                [
                    {
                        "parameter_id": "date_range",
                        "card_id": card_id,
                        "target": ["dimension", ["template-tag", "date_range"]],
                    }
                ]
                if card_name in CARD_DATE_FIELDS
                else []
            ),
        })

    api(
        "put",
        f"/api/dashboard/{dash_id}",
        token,
        json={
            "dashcards": cards_payload,
            "tabs": tabs_payload,
            "parameters": DASHBOARD_PARAMETERS,
            "width": "fixed",
        },
    )
    log.info("Dashboard synced with %d cards across %d sections (id=%d).", len(cards), len(SECTIONS), dash_id)
    return dash_id


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    wait_for_metabase()
    token = setup_metabase()
    db_id = add_database(token)
    cards = create_cards(token, db_id)
    dash_id = create_dashboard(token, cards)
    log.info(
        "Seeding complete. Open your dashboard at %s/dashboard/%d",
        PUBLIC_BASE_URL, dash_id,
    )


if __name__ == "__main__":
    main()
