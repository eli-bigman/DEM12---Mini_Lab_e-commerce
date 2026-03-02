# Architecture & Developer Reference

## Mini Data Platform — E-Commerce Sales & Operations Analytics System

This document is the authoritative technical reference for the platform. It covers every component, explains what each file does, describes how all the pieces connect, and provides guidance for making changes.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Repository Structure](#2-repository-structure)
3. [Component Deep-Dives](#3-component-deep-dives)
   - 3.1 [Collector Service](#31-collector-service)
   - 3.2 [MinIO Object Storage](#32-minio-object-storage)
   - 3.3 [Apache Airflow](#33-apache-airflow)
   - 3.4 [PostgreSQL](#34-postgresql)
   - 3.5 [Metabase](#35-metabase)
   - 3.6 [GitHub Actions CI/CD](#36-github-actions-cicd)
4. [Data Flow End-to-End](#4-data-flow-end-to-end)
5. [Database Schema Reference](#5-database-schema-reference)
6. [Airflow DAG Reference](#6-airflow-dag-reference)
7. [Environment Variables](#7-environment-variables)
8. [Design Decisions Explained](#8-design-decisions-explained)
9. [How to Make Common Changes](#9-how-to-make-common-changes)

---

## 1. System Overview

The platform is a self-contained, fully containerised data engineering pipeline that simulates a live e-commerce operation. It has five logical layers:

```
[ Collector ] --> [ MinIO ] --> [ Airflow ] --> [ PostgreSQL ] --> [ Metabase ]
  Generate         Store          Process          Model              Visualise
```

| Layer | What it does |
|---|---|
| **Ingestion** | The Collector constantly generates synthetic transactional data and uploads it to MinIO as CSV files. |
| **Data Lake** | MinIO holds raw CSV files in time-partitioned folders, organised by entity and date. |
| **Orchestration** | Airflow detects new files, validates them, cleans them, loads them into a staging area, and then transforms the data into a star schema. |
| **Structured Storage** | PostgreSQL holds two schemas: `staging` (landing zone) and `analytics` (star schema for querying). |
| **Business Intelligence** | Metabase connects to the `analytics` schema and presents executive KPI cards and trend charts. |

All eight Docker containers are defined in `docker-compose.yml` and communicate over a single internal bridge network called `platform_net`. No container is directly accessible from outside Docker except through the mapped ports listed in the README.

---

## 2. Repository Structure

```
dem12-mini-lab/
│
├── .github/
│   └── workflows/
│       └── ci.yml                   # GitHub Actions CI/CD pipeline
│
├── airflow/
│   ├── Dockerfile                   # Custom Airflow image (adds providers + requirements)
│   ├── requirements.txt             # Airflow extra Python deps (minio, psycopg2, etc.)
│   └── dags/
│       ├── ecommerce_pipeline.py    # Main ETL DAG
│       ├── platform_health_check.py # Connectivity validation DAG
│       └── utils/
│           ├── __init__.py
│           ├── minio_helper.py      # MinIO: list, read, quarantine
│           ├── db_helper.py         # PostgreSQL: connections, idempotency gate
│           ├── validators.py        # Validation: per-entity rules
│           ├── cleaners.py          # Cleaning: type coercion, standardisation
│           ├── loaders.py           # Loading: staging UPSERT functions
│           └── transformers.py      # Transforms: star schema upserts (SCD2/CDC)
│
├── collector/
│   ├── Dockerfile                   # Minimal Python 3.11 image
│   ├── requirements.txt             # faker, minio
│   ├── main.py                      # Entry point — runs batch loop indefinitely
│   ├── uploader.py                  # Packages DataFrames as CSV and uploads to MinIO
│   └── generators/
│       ├── __init__.py
│       ├── customers.py             # Generates customer profile rows
│       ├── products.py              # Generates product catalog rows
│       ├── orders.py                # Generates order rows (FK to customers + products)
│       ├── payments.py              # Generates payment rows (FK to orders)
│       ├── inventory.py             # Generates inventory rows (FK to products)
│       ├── revenue.py               # Generates daily revenue aggregate rows
│       └── returns.py               # Generates return rows (FK to orders)
│
├── metabase/
│   ├── requirements.txt             # requests (for Metabase REST API)
│   └── seed.py                      # Provisions Metabase via API: DB, cards, dashboard
│
├── postgres/
│   └── init/
│       ├── 01_init.sql              # Roles, schemas, extensions
│       ├── 02_staging_schema.sql    # Staging tables (landing zone)
│       ├── 03_analytics_schema.sql  # Star schema: dims, facts, partitions, dim_date
│       └── 04_analytics_views.sql   # Dashboard views (back the Metabase cards)
│
├── tests/
│   ├── __init__.py
│   ├── conftest.py                  # Shared pytest fixtures (valid sample DataFrames)
│   ├── test_validators.py           # 22 validator unit tests
│   ├── test_cleaners.py             # 18 cleaner unit tests
│   └── test_dag_integrity.py        # DAG import + structure tests
│
├── .env                             # Live credentials (git-ignored)
├── .env.example                     # Credential template committed to repo
├── .gitignore
├── docker-compose.yml               # All 8 services
├── Makefile                         # Developer shortcuts
├── README.md                        # Getting started guide
├── requirements-dev.txt             # pytest + pandas for running tests locally
└── ARCHITECTURE.md                  # This file
```

---

## 3. Component Deep-Dives

---

### 3.1 Collector Service

**Files:** `collector/main.py`, `collector/uploader.py`, `collector/generators/*.py`

**Purpose:** Continuously generates realistic synthetic e-commerce data and uploads it to MinIO. It simulates a live transactional system producing data 24/7.

#### How it works

`main.py` is the entry point. It runs an infinite loop:

```python
while True:
    batch_customers = generate_customers(BATCH_SIZE)
    batch_products  = generate_products(BATCH_SIZE)
    batch_orders    = generate_orders(BATCH_SIZE, customer_ids, product_ids)
    ...
    upload_to_minio(entity, dataframe)
    sleep(BATCH_INTERVAL_SECONDS)
```

Each iteration produces one CSV file per entity. The loop is intentional — the Collector is designed to run forever until the container is stopped.

#### Generators

Each generator in `generators/` is a pure function that returns a pandas DataFrame. They use the `faker` library to produce realistic names, countries, and dates.

A critical design choice: **entities are linked within each batch**. The `orders` generator receives the `customer_ids` and `product_ids` from the same batch and uses `random.choice()` to assign FKs. Similarly, `payments` and `returns` receive `order_ids`. This ensures referential integrity in the generated data.

| Generator | Key fields | Linked to |
|---|---|---|
| `customers` | customer_id (UUID), country, acquisition_channel, customer_segment | — |
| `products` | product_id (UUID), category, cost_price, selling_price | — |
| `orders` | order_id, customer_id, product_id, total_amount, order_status | customers, products |
| `payments` | payment_id, order_id, amount, payment_status | orders |
| `inventory` | inventory_id, product_id, quantity_on_hand | products |
| `revenue` | revenue_id, record_date, total_revenue, total_profit | — |
| `returns` | return_id, order_id, return_reason, return_status | orders |

#### Uploader

`uploader.py` takes a DataFrame, serialises it to CSV in memory (never touches disk), and calls the MinIO SDK's `put_object()`. The destination path is always time-partitioned:

```
raw-data/{entity}/YYYY/MM/DD/{entity}_YYYYMMDD_HHMMSS.csv
```

This partitioning scheme means Airflow can scan for files by prefix (e.g. `orders/2024/06/`) without listing the entire bucket.

#### Configuration

| Variable | Default | Effect |
|---|---|---|
| `COLLECTOR_BATCH_SIZE` | `50` | Rows per entity per batch |
| `COLLECTOR_BATCH_INTERVAL_SECONDS` | `60` | Seconds between batches |

---

### 3.2 MinIO Object Storage

**Services:** `minio`, `minio-init` in `docker-compose.yml`

**Purpose:** Acts as the data lake — a persistent, S3-compatible object store where raw CSV files live until Airflow processes them.

#### How it works

The `minio` container runs the MinIO server. It stores files on a Docker volume (`minio_data`) so data persists across restarts.

The `minio-init` container runs once on startup using the `mc` (MinIO Client) CLI tool. It:
1. Waits for MinIO to be healthy
2. Configures an alias pointing at the MinIO server
3. Creates the `raw-data` bucket (for the Collector to write to)
4. Creates the `quarantine` bucket (for Airflow DLQ writes)

#### Bucket structure

```
raw-data/               <- Collector writes here
  customers/YYYY/MM/DD/customers_YYYYMMDD_HHMMSS.csv
  products/YYYY/MM/DD/
  orders/YYYY/MM/DD/
  payments/YYYY/MM/DD/
  inventory/YYYY/MM/DD/
  revenue/YYYY/MM/DD/
  returns/YYYY/MM/DD/

quarantine/             <- Airflow writes invalid rows here (DLQ)
  customers/YYYY/MM/DD/customers_YYYYMMDD_HHMMSS_quarantine.csv
  ...
```

#### Access

- **MinIO Console:** http://localhost:9001 — browser UI to browse buckets, inspect files, view object metadata
- **MinIO API:** http://localhost:9000 — S3-compatible endpoint used by the Collector and Airflow

---

### 3.3 Apache Airflow

**Services:** `airflow-webserver`, `airflow-scheduler`, `airflow-init` in `docker-compose.yml`

**Files:** `airflow/Dockerfile`, `airflow/requirements.txt`, `airflow/dags/`

**Purpose:** Orchestrates the entire data processing pipeline. Detects new files in MinIO every hour, validates them, cleans them, loads them into PostgreSQL staging, and then transforms staging data into the analytics star schema.

#### Docker setup

The `airflow/Dockerfile` starts from the official Apache Airflow 2.9.3 image and installs the additional Python packages listed in `airflow/requirements.txt`:

- `minio` — MinIO Python SDK for reading files from object storage
- `psycopg2-binary` — PostgreSQL adapter for Python
- `pandas` — data manipulation inside DAG tasks
- `great-expectations` — listed for future validation enhancement

The `airflow-init` service runs once on first startup. It:
1. Runs `airflow db migrate` to apply the Airflow schema to PostgreSQL
2. Runs `airflow users create` to create the admin UI user using credentials from `.env`

#### The `ecommerce_pipeline` DAG

**File:** `airflow/dags/ecommerce_pipeline.py`

This is the main DAG. It uses the TaskFlow API (`@task` decorators) and runs `@hourly`.

**Task graph:**

```
ingest_customers  ──┐
ingest_products   ──┤
ingest_orders     ──┤
ingest_payments   ──┤──> transform_to_analytics
ingest_inventory  ──┤
ingest_revenue    ──┤
ingest_returns    ──┘
```

The seven ingest tasks run **in parallel** (no dependencies between them). They all fan into `transform_to_analytics` which runs after all seven succeed.

`max_active_runs=1` prevents concurrent DAG runs from racing over the same files.

#### Utility modules (`airflow/dags/utils/`)

Each module has a single responsibility:

---

**`minio_helper.py`**

Provides three functions:

- `get_client()` — reads `MINIO_ENDPOINT`, `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD` from env and returns an authenticated MinIO SDK client.
- `list_objects(client, bucket, prefix)` — lists all objects under a prefix. Used to scan `raw-data/{entity}/`.
- `read_csv(client, bucket, path)` — downloads an object into memory and returns a pandas DataFrame. All columns are read as strings at this stage — type enforcement is done by the cleaners.
- `send_to_quarantine(client, entity, source_path, df)` — serialises a DataFrame of invalid rows to CSV in memory and uploads it to the `quarantine/` bucket.

---

**`db_helper.py`**

Provides:

- `get_connection()` — creates a psycopg2 connection from env vars. Autocommit is off by default.
- `transaction()` — a context manager. Yields a cursor, commits on success, rolls back on any exception, and always closes the connection.
- `execute_values(sql, rows)` — bulk-inserts a list of tuples using `psycopg2.extras.execute_values` for efficiency.
- `file_already_processed(path)` — queries `staging.processed_files` and returns True if the file has already been loaded. **This is the idempotency gate.** If Airflow retries a failed task, this prevents double-loading.
- `mark_file_processed(path, entity, row_count, ...)` — inserts a record into `staging.processed_files` once a file has been successfully loaded.

---

**`validators.py`**

Contains one validation function per entity (e.g. `validate_customers`, `validate_orders`) and a `validate(entity, df)` dispatcher.

Each function applies a set of pandas boolean masks:

| Check | Implementation |
|---|---|
| Required fields not null | `df[col].notna() & (df[col].str.strip() != "")` |
| UUID format | regex match against `[0-9a-f]{8}-[0-9a-f]{4}-...-[0-9a-f]{12}` |
| Enum allowlists | `df[col].isin({"Value1", "Value2"})` |
| Positive numerics | `pd.to_numeric(series, errors="coerce") >= 0` |
| No duplicates in batch | `~df.duplicated(subset=[id_col], keep="first")` |

Returns a `(valid_df, invalid_df)` tuple. The DAG uses `invalid_df` to send records to the DLQ and `valid_df` to continue processing.

---

**`cleaners.py`**

Contains one cleaning function per entity and a `clean(entity, df)` dispatcher. Runs after validation on the valid subset only.

Operations per entity:

| Operation | Example |
|---|---|
| Strip whitespace | `df[str_cols].str.strip()` |
| Parse timestamps to UTC | `pd.to_datetime(series, utc=True)` |
| Parse dates | `pd.to_datetime(series).dt.date` |
| Coerce to numeric | `pd.to_numeric(series).round(2)` |
| Coerce to int | `pd.to_numeric(series).fillna(0).astype(int)` |
| Standardise case | `.str.upper()` for country codes, `.str.title()` for categories |

---

**`loaders.py`**

Contains one UPSERT function per entity and a `load_to_staging(entity, df, source_file)` dispatcher.

Each function builds a list of tuples from the DataFrame rows and calls `execute_values()` with an `INSERT ... ON CONFLICT DO UPDATE` statement. The ON CONFLICT target is always the entity's natural primary key (e.g. `order_id`).

**CDC in the loader:** For `orders`, the ON CONFLICT clause updates `order_status`. This means if the Collector later uploads a file where the same `order_id` has `order_status = 'Refunded'` (having previously been `'Completed'`), the staging table will reflect the new status. The analytical transform then propagates this to `fact_orders`.

---

**`transformers.py`**

Contains one transform function per analytics table and a `run_all_transforms()` orchestrator.

**`transform_dim_customers()` and `transform_dim_products()` — SCD Type 2:**

1. Find the latest version of each entity in staging (using `DISTINCT ON ... ORDER BY _loaded_at DESC`)
2. `UPDATE` any current dimension rows where tracked attributes have changed — set `valid_to = NOW()`, `is_current = FALSE`
3. `INSERT` new current rows for any customer/product that was just expired OR is completely new

**`transform_dim_inventory()` — snapshot:**
Simple `INSERT ... ON CONFLICT DO UPDATE` that always overwrites with the latest values.

**`transform_fact_orders()` — CDC-aware:**
Joins staging.orders to the current dimension surrogate keys, computes `profit = revenue - (cost_price * quantity)`, then does an `INSERT ... ON CONFLICT (order_id) DO UPDATE` that updates `order_status`, `revenue`, and `profit` if the row already exists.

**`transform_agg_revenue()` — delete+insert:**
Deletes from `agg_revenue` any dates present in staging, then re-inserts by aggregating revenue + profit from staging.revenue and counting completed orders from staging.orders.

---

**`platform_health_check.py`**

A simple validation DAG (not scheduled, triggered manually) that verifies:
- MinIO is reachable and the `raw-data` bucket exists
- PostgreSQL is reachable and the `staging` schema exists

Useful for confirming connectivity after infrastructure changes.

---

### 3.4 PostgreSQL

**Service:** `postgres` in `docker-compose.yml`

**Files:** `postgres/init/01_init.sql` through `04_analytics_views.sql`

**Purpose:** The structured storage layer. Holds two schemas — `staging` (landing zone) and `analytics` (query-ready star schema) — and the Airflow metadata database on a separate database.

#### Initialisation order

PostgreSQL runs all `.sql` files in `postgres/init/` alphabetically on first start:

1. `01_init.sql` — Creates the `ecommerce` database, sets up roles, creates the `staging` and `analytics` schemas, installs the `uuid-ossp` extension, and creates a read-only `metabase_user` role.
2. `02_staging_schema.sql` — Creates all 8 staging tables.
3. `03_analytics_schema.sql` — Creates the full star schema including monthly partitions for `fact_orders` and pre-populates `dim_date`.
4. `04_analytics_views.sql` — Creates 7 SQL views that power the Metabase dashboard cards.

> **Important:** If you need to modify the schema, do not re-run init scripts manually against a live DB — the PostgreSQL init directory only runs on a fresh volume. To apply changes to a running DB, write and apply a migration script manually or via `make psql`.

---

### 3.5 Metabase

**Service:** `metabase` in `docker-compose.yml`

**Files:** `metabase/seed.py`, `metabase/requirements.txt`

**Purpose:** The business intelligence layer. Provides an interactive dashboard UI that non-technical users can use to view KPIs and trends.

#### How Metabase connects to PostgreSQL

Metabase connects as `metabase_user` which has `SELECT`-only privileges on the `analytics` schema. This is set up in `01_init.sql` and enforced at the role level — Metabase cannot write to or drop any tables.

#### Dashboard seeding

`metabase/seed.py` uses the Metabase REST API to provision everything automatically:

1. Polls `/api/health` until Metabase is ready
2. Hits `/api/session/properties` to get the setup token
3. Posts to `/api/setup` to create the admin user (only first time)
4. Logs in to get a session token
5. Posts to `/api/database` to add the PostgreSQL connection
6. Creates 11 question cards (one per view + KPI metric) via `/api/card`
7. Creates the dashboard via `/api/dashboard`
8. Adds all cards to the dashboard with a two-column grid layout

All of these steps are idempotent — if a card or database already exists with the same name, `seed.py` uses its existing ID rather than creating a duplicate.

#### The seven dashboard views

Each view is defined in `04_analytics_views.sql` and targets one card on the executive dashboard:

| View | SQL strategy | Business question |
|---|---|---|
| `v_kpi_summary` | Single aggregation row with window functions | Revenue, orders, AOV, profit, customer growth this month |
| `v_revenue_trend_daily` | LEFT JOIN dim_date ← fact_orders GROUP BY date | How is revenue trending over time? |
| `v_revenue_by_category` | JOIN products → GROUP BY category | Which product categories drive the most revenue? |
| `v_revenue_by_channel` | JOIN customers → GROUP BY acquisition_channel | Which marketing channel generates the most revenue? |
| `v_top_products_profit` | GROUP BY product LIMIT 10 ORDER BY profit | Which specific products are most profitable? |
| `v_customer_retention` | JOIN customers → GROUP BY segment | What's the balance of New vs Returning vs VIP customers? |
| `v_order_status_distribution` | GROUP BY order_status | What fraction of orders are completed vs cancelled vs refunded? |

---

### 3.6 GitHub Actions CI/CD

**File:** `.github/workflows/ci.yml`

**Purpose:** Automatically validate the entire codebase on every push and pull request to `main`/`master`.

#### Three-job pipeline

**Job 1: `test`**
- Installs Python 3.11 and `requirements-dev.txt`
- Installs Apache Airflow (so DAG integrity tests can use `DagBag`)
- Runs `pytest tests/test_validators.py tests/test_cleaners.py`
- Fast — no Docker required, runs in about 30 seconds

**Job 2: `build`** (depends on `test`)
- Copies `.env.example` to `.env`
- Runs `docker compose build` to verify all Dockerfiles build successfully
- Catches image layer errors, missing files, and broken requirements

**Job 3: `validate`** (depends on `build`)
- Starts the full stack with `docker compose up -d --build`
- Waits 90 seconds for all services (including Airflow init) to become healthy
- Verifies PostgreSQL is ready with `pg_isready`
- Confirms all analytics tables exist
- Triggers the `ecommerce_pipeline` DAG manually
- Waits 120 seconds for the pipeline to process data
- Queries `analytics.fact_orders` and fails the job if `COUNT(*) = 0`
- Tears down the stack on completion (even on failure)

---

## 4. Data Flow End-to-End

Here is the full path of a single row of data from generation to dashboard:

### Step 1: Generation (Collector)

```
collector/main.py → generators/orders.py
→ DataFrame: {order_id, customer_id, product_id, quantity, unit_price, total_amount,
              order_timestamp, payment_method, order_status}
```

### Step 2: Upload to MinIO (Collector)

```
collector/uploader.py → MinIO SDK put_object()
→ s3://raw-data/orders/2024/06/15/orders_20240615_140000.csv
```

### Step 3: File detection (Airflow)

```
ecommerce_pipeline.py → ingest_orders task
→ minio_helper.list_objects(bucket="raw-data", prefix="orders/")
→ filters to: ["orders/2024/06/15/orders_20240615_140000.csv"]
→ db_helper.file_already_processed("orders/2024/06/15/orders_...") → False
→ proceeds
```

### Step 4: Read from MinIO (Airflow)

```
minio_helper.read_csv(...)
→ pandas DataFrame (all columns as str, 50 rows)
```

### Step 5: Validation (Airflow)

```
validators.validate_orders(df)
→ valid_df   (e.g. 48 rows — all fields present, valid UUID, status in allowed set)
→ invalid_df (e.g. 2 rows — null customer_id or negative quantity)
```

### Step 6: DLQ (Airflow)

```
minio_helper.send_to_quarantine(client, entity="orders", source_path=..., df=invalid_df)
→ s3://quarantine/orders/2024/06/15/orders_20240615_140000_quarantine.csv
```

### Step 7: Cleaning (Airflow)

```
cleaners.clean_orders(valid_df)
→ order_timestamp parsed to UTC pandas Timestamp
→ unit_price / total_amount coerced to float64, rounded to 2dp
→ quantity coerced to int
→ order_status title-cased ("completed" → "Completed")
```

### Step 8: Staging load (Airflow)

```
loaders.load_orders(cleaned_df, source_file="orders/2024/06/15/...")
→ execute_values() with INSERT INTO staging.orders ... ON CONFLICT (order_id) DO UPDATE
→ 48 rows upserted into staging.orders
```

### Step 9: Mark processed (Airflow)

```
db_helper.mark_file_processed("orders/2024/06/15/...", entity="orders",
    row_count=48, quarantine_count=2, status="partial")
→ INSERT INTO staging.processed_files ...
```

### Step 10: Analytics transform (Airflow — after all 7 entities are staged)

```
transformers.transform_dim_customers()   → analytics.dim_customers upserted (SCD2)
transformers.transform_dim_products()    → analytics.dim_products upserted (SCD2)
transformers.transform_dim_inventory()   → analytics.dim_inventory upserted (snapshot)
transformers.transform_fact_orders()     → analytics.fact_orders upserted
                                           (joins to current dim rows for surrogate keys)
transformers.transform_fact_payments()   → analytics.fact_payments upserted
transformers.transform_fact_returns()    → analytics.fact_returns upserted
transformers.transform_agg_revenue()     → analytics.agg_revenue rebuilt for affected dates
```

### Step 11: Dashboard query (Metabase)

```
Metabase UI → SELECT * FROM analytics.v_kpi_summary
→ Single row: {total_revenue: 124580.00, total_orders: 1204, avg_order_value: 103.47, ...}
→ Rendered as KPI cards on the executive dashboard
```

---

## 5. Database Schema Reference

### `staging` schema

Tables are append-friendly landing zones. All primary keys use the entity's natural business UUID as the primary key together with `ON CONFLICT DO UPDATE` for idempotency. Every table tracks `_loaded_at` and `_source_file` for lineage.

| Table | Primary Key | Purpose |
|---|---|---|
| `staging.customers` | `customer_id` | Raw customer profiles |
| `staging.products` | `product_id` | Raw product catalog |
| `staging.orders` | `order_id` | Raw order transactions |
| `staging.payments` | `payment_id` | Raw payment records |
| `staging.inventory` | `inventory_id` | Raw inventory snapshots |
| `staging.revenue` | `revenue_id` | Raw daily revenue rows |
| `staging.returns` | `return_id` | Raw return records |
| `staging.processed_files` | `object_path` | Idempotency registry of loaded MinIO files |

### `analytics` schema — Star Schema

**Dimension tables:**

| Table | Key type | SCD type | Notes |
|---|---|---|---|
| `dim_date` | `date_key` INTEGER (YYYYMMDD) | none | Pre-populated 2024-01-01 to 2026-12-31 |
| `dim_customers` | `customer_sk` BIGSERIAL | SCD Type 2 | Tracks segment/channel/country changes |
| `dim_products` | `product_sk` BIGSERIAL | SCD Type 2 | Tracks category/price changes |
| `dim_inventory` | `inventory_sk` BIGSERIAL | Snapshot | Always reflects current stock levels |

**SCD Type 2 columns on dim_customers and dim_products:**

```sql
valid_from   TIMESTAMPTZ  -- When this version became active
valid_to     TIMESTAMPTZ  -- NULL means currently active
is_current   BOOLEAN      -- TRUE for the active version
```

To query only current data: `WHERE is_current = TRUE`
To query historical data as of a date: `WHERE valid_from <= {date} AND (valid_to > {date} OR valid_to IS NULL)`

**Fact tables:**

| Table | Primary Key | Partitioning | Notes |
|---|---|---|---|
| `fact_orders` | `order_id` | `RANGE (order_timestamp)` monthly | Contains profit computed as revenue − (cost × qty) |
| `fact_payments` | `payment_id` | none | |
| `fact_returns` | `return_id` | none | |
| `agg_revenue` | `record_date` | none | Rebuilt daily; also joins to staging.orders for order counts |

**Partition structure for `fact_orders`:**

Monthly child tables are created automatically by the `DO $$` block in `03_analytics_schema.sql`:
```
analytics.fact_orders_2024_01  -- Jan 2024
analytics.fact_orders_2024_02  -- Feb 2024
...
analytics.fact_orders_2026_12  -- Dec 2026
analytics.fact_orders_default  -- catch-all for any out-of-range rows
```

PostgreSQL automatically routes an `INSERT` into `fact_orders` to the correct monthly child table based on `order_timestamp`. Queries against the parent table automatically scan only the relevant partitions when a date range filter is applied (partition pruning).

---

## 6. Airflow DAG Reference

### `ecommerce_pipeline`

| Property | Value |
|---|---|
| Schedule | `@hourly` |
| Start date | `2024-01-01` |
| Catchup | `False` (only runs from now forward) |
| Max active runs | `1` (no concurrent runs) |
| Retries | 2 per task, 5-minute delay |

**Task dependency graph:**

```
ingest_customers ─┐
ingest_products  ─┤
ingest_orders    ─┤
ingest_payments  ─┼──► transform_to_analytics
ingest_inventory  ─┤
ingest_revenue   ─┤
ingest_returns   ─┘
```

**What each ingest task does** (they all do the same thing, for their entity):

```
1. Get MinIO client
2. List all objects under raw-data/{entity}/
3. Filter to objects not in staging.processed_files
4. For each new object:
   a. read_csv() → DataFrame
   b. validate() → (valid_df, invalid_df)
   c. send_to_quarantine() if invalid_df is not empty
   d. clean() → cleaned_df
   e. load_to_staging() → UPSERT
   f. mark_file_processed()
5. Return summary dict for XCom
```

**What `transform_to_analytics` does:**

Calls `run_all_transforms()` which executes all seven transform SQL statements in the correct dependency order (dimensions first, then facts that reference them).

---

### `platform_health_check`

| Property | Value |
|---|---|
| Schedule | None (manual trigger only) |
| Purpose | Verify MinIO and PostgreSQL are accessible |

Run it via:
```bash
make trigger DAG=platform_health_check
# or
docker exec ecommerce_airflow_webserver airflow dags trigger platform_health_check
```

---

## 7. Environment Variables

All variables live in `.env`. Copy from `.env.example` before first run.

### PostgreSQL

| Variable | Used by | Description |
|---|---|---|
| `POSTGRES_HOST` | Airflow, Collector | Hostname inside Docker network (`postgres`) |
| `POSTGRES_PORT` | Airflow | Port (5432) |
| `POSTGRES_DB` | All | Database name (`ecommerce`) |
| `POSTGRES_USER` | Airflow, Init | Main app user |
| `POSTGRES_PASSWORD` | Airflow, Init | Main app password |
| `MB_DB_USER` | Metabase | Read-only Metabase role |
| `MB_DB_PASS` | Metabase, seed.py | Metabase role password |

### MinIO

| Variable | Used by | Description |
|---|---|---|
| `MINIO_ROOT_USER` | Collector, Airflow, MinIO | Admin access key |
| `MINIO_ROOT_PASSWORD` | Collector, Airflow, MinIO | Admin secret key |
| `MINIO_ENDPOINT` | Collector, Airflow | Internal endpoint (`minio:9000`) |
| `MINIO_RAW_BUCKET` | Airflow | Source bucket name (default: `raw-data`) |

### Airflow

| Variable | Used by | Description |
|---|---|---|
| `AIRFLOW__CORE__FERNET_KEY` | Airflow | Encryption key for stored connections/passwords |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | Airflow | Airflow's own metadata DB connection string |
| `AIRFLOW_ADMIN_USERNAME` | airflow-init | Admin UI username |
| `AIRFLOW_ADMIN_PASSWORD` | airflow-init | Admin UI password |
| `AIRFLOW_ADMIN_EMAIL` | airflow-init | Admin UI email |
| `AIRFLOW_UID` | Airflow containers | UID for file permissions (set to your local user UID on Linux) |

### Collector

| Variable | Used by | Description |
|---|---|---|
| `COLLECTOR_BATCH_SIZE` | collector | Rows per entity per batch |
| `COLLECTOR_BATCH_INTERVAL_SECONDS` | collector | Sleep time between batches |

### Metabase

| Variable | Used by | Description |
|---|---|---|
| `METABASE_ADMIN_EMAIL` | seed.py | Admin email for dashboard seeding |
| `METABASE_ADMIN_PASSWORD` | seed.py | Admin password for dashboard seeding |
| `METABASE_URL` | seed.py | Metabase base URL (default: `http://localhost:3000`) |

---

## 8. Design Decisions Explained

### Why MinIO instead of writing directly to PostgreSQL?

Storage before transformation (the "lake before warehouse" pattern) decouples the Collection layer from the Processing layer. The Collector never needs to know about the PostgreSQL schema. If the schema changes, the Collector keeps working and only Airflow needs updating. Raw files in MinIO also serve as an audit trail and allow re-processing from scratch if a bug is found in the transformation logic.

### Why SCD Type 2 for customers and products?

Slowly Changing Dimensions (SCD2) preserve historical accuracy. If a customer moves from segment `New` to `VIP`, the old orders should still show them as `New` (because that was their state at the time of purchase). Without SCD2, any historical analysis of "revenue by customer segment" would be distorted by retroactively applying current segment values.

### Why stage before transforming?

The staging tables provide a clean, always-available copy of the last state of the raw data. If the analytics transform fails, the data is not lost — it's in staging and the transform can be re-run. It also means Airflow tasks are smaller and more focused: ingest tasks do not need to know about the star schema.

### Why `INSERT ... ON CONFLICT` instead of DELETE+INSERT for staging?

UPSERT is more efficient and handles concurrent loads better. For staging, we always want the latest state (e.g. if `order_status` changes from `Completed` to `Refunded`, we want the staging row updated, not duplicated).

### Why DELETE+INSERT for `agg_revenue`?

`agg_revenue` is an aggregation over staging data for each date. If new revenue rows arrive for a date that already has an aggregate row, we cannot simply UPSERT the aggregate — we need to recompute the entire aggregation for that date. DELETE the existing aggregate rows for affected dates, then re-INSERT the freshly computed values.

### Why partition `fact_orders` monthly?

Orders is likely the largest and most queried table. Monthly partitioning means that a query for "orders in Q3 2024" will only scan three child tables rather than the entire table. PostgreSQL performs partition pruning automatically when a date filter matches the partition key column (`order_timestamp`). The `default` partition ensures no rows are ever lost if an out-of-range date appears.

### Why is `AIRFLOW_UID` set?

On Linux, if Airflow runs as root inside the container but the `airflow/logs` and `airflow/plugins` directories are owned by a different user on the host, Airflow will fail to write logs. Setting `AIRFLOW_UID` to `$(id -u)` on Linux makes the container user match the host user. On Windows/macOS with Docker Desktop, this is not required but also does no harm.

---

## 9. How to Make Common Changes

### Add a new data entity (e.g. `supplier`)

1. **Collector:** Add `collector/generators/supplier.py` with a `generate_suppliers(n)` function returning a DataFrame. Import and call it in `collector/main.py`.
2. **MinIO:** No changes needed — the bucket already accepts any prefix.
3. **Staging schema:** Add a `staging.suppliers` table to `02_staging_schema.sql`. Apply it via:
   ```bash
   make psql
   -- paste the CREATE TABLE statement manually
   ```
4. **Validators:** Add `validate_suppliers()` to `validators.py` and add it to the `VALIDATORS` dict.
5. **Cleaners:** Add `clean_suppliers()` to `cleaners.py` and add it to `CLEANERS`.
6. **Loaders:** Add `load_suppliers()` to `loaders.py` and add it to `LOADERS`.
7. **Airflow DAG:** Add `ingest_suppliers` task and add it to the fan-in list before `transform_to_analytics`.
8. **Analytics schema (optional):** Add a `dim_suppliers` table to `03_analytics_schema.sql` if needed and update `transformers.py`.
9. **Tests:** Add fixtures and test cases to `conftest.py`, `test_validators.py`, and `test_cleaners.py`.

### Change the batch size or frequency

Edit `.env`:
```
COLLECTOR_BATCH_SIZE=100
COLLECTOR_BATCH_INTERVAL_SECONDS=30
```
Then restart the collector:
```bash
docker compose restart collector
```

### Add a new Metabase dashboard card

1. Write a new view in `04_analytics_views.sql`:
   ```sql
   CREATE OR REPLACE VIEW analytics.v_my_new_metric AS
   SELECT ...
   ```
   Apply it via `make psql`.
2. Add a new entry to the `CARDS` list in `metabase/seed.py`:
   ```python
   {"name": "My New Metric", "display": "scalar", "query": "SELECT ... FROM analytics.v_my_new_metric"},
   ```
3. Re-run:
   ```bash
   make seed-metabase
   ```
   The script will create the new card and add it to the existing dashboard (idempotent).

### Modify the SCD2 tracked attributes

In `transformers.py`, `transform_dim_customers()`, find the `IS DISTINCT FROM` clauses in the `expired` CTE and add or remove the attributes you want to track for type 2 history:

```sql
dp.category      IS DISTINCT FROM s.category OR
dp.cost_price    IS DISTINCT FROM s.cost_price OR
dp.selling_price IS DISTINCT FROM s.selling_price  -- add or remove lines here
```

### Reset the entire platform (wipe all data)

```bash
make nuke
make up
```

This removes all Docker volumes (MinIO data, PostgreSQL data, Airflow logs) and rebuilds the stack from scratch.

### Apply a schema change to a running database

PostgreSQL `init/` scripts only run on a fresh volume. For a running database, connect via psql and apply the DDL manually:

```bash
make psql
-- Run your ALTER TABLE, CREATE INDEX, etc.
\q
```

---

*Last updated: 2026-03-02*
