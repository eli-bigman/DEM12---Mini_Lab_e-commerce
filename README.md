# Mini Data Platform: E-Commerce Sales & Operations Analytics System

A containerized, end-to-end data engineering platform that simulates a real-world e-commerce system. The platform demonstrates batch data ingestion, orchestrated processing, structured analytics modeling, and executive-level BI dashboards.

---

## Architecture

The system consists of five core services orchestrated via Docker Compose:

| Service | Technology | Role |
|---|---|---|
| Collector | Python | Simulates e-commerce activity and generates raw data |
| Data Lake | MinIO | Object storage for raw time-partitioned CSV files |
| Orchestrator | Apache Airflow | Validates, cleans, and loads data into PostgreSQL |
| Analytics Store | PostgreSQL | Structured storage in a star schema |
| BI Layer | Metabase | Executive dashboards and KPI reporting |

---

## Project Structure

```
.
├── airflow/
│   ├── dags/           # Airflow DAG definitions
│   ├── Dockerfile      # Airflow image with custom dependencies
│   └── requirements.txt
├── collector/
│   ├── generators/     # Per-entity data generators
│   ├── Dockerfile
│   ├── main.py         # Collector entrypoint
│   ├── uploader.py     # MinIO upload logic
│   └── requirements.txt
├── minio/
│   └── init/           # Bucket initialization scripts
├── postgres/
│   └── init/           # Database and role initialization SQL
├── .env.example        # Environment variable template
├── docker-compose.yml  # Full stack orchestration
└── README.md
```

---

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (v20+)
- [Docker Compose](https://docs.docker.com/compose/) (v2+)

---

## Setup & Running

**1. Clone the repository:**

```bash
git clone <your-repo-url>
cd dem12-mini-lab
```

**2. Configure environment variables:**

```bash
cp .env.example .env
# Edit .env with your preferred credentials
```

**3. Start the full stack:**

```bash
docker compose up -d --build
```

**4. Access the services:**

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | See `.env` |
| Metabase | http://localhost:3000 | Initial setup on first launch |
| PostgreSQL | localhost:5432 | See `.env` |

---

## Data Flow

```
[Collector] --> [MinIO: raw-data/] --> [Airflow DAGs] --> [PostgreSQL: Star Schema] --> [Metabase]
```

1. The **Collector** generates micro-batches of transactional data every 60 seconds.
2. Files are uploaded to **MinIO** under time-partitioned paths: `raw-data/{entity}/YYYY/MM/DD/`.
3. **Airflow** detects new files, validates with Great Expectations, applies cleaning/transformation, and loads into PostgreSQL.
4. **Metabase** connects to PostgreSQL and provides executive dashboards.

---

## Data Entities

The collector generates seven entities per batch:

- `customers` - User profiles with acquisition channel and segment
- `products` - Catalog with cost and selling prices
- `orders` - Purchase transactions with status
- `payments` - Payment records linked to orders
- `inventory` - Stock levels per warehouse
- `revenue` - Daily aggregated revenue snapshots
- `returns` - Customer refund records

---

## Non-Functional Considerations

- All secrets are externalized to `.env` (never committed).
- All Docker services include health checks.
- Data processing pipelines are idempotent (UPSERT patterns).
- Separation of concerns: ingestion and processing are fully decoupled.
- Repository is structured for CI/CD via GitHub Actions (Phase 6).
