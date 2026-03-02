# ============================================================
# Makefile for the Mini Data Platform
#
# Usage:   make <target>
# Example: make up
# ============================================================

.PHONY: help up down restart build logs ps \
        init-db seed-metabase \
        trigger-dag dag-logs \
        test test-unit test-dag \
        clean nuke

# Default target
help:
	@echo ""
	@echo "Mini Data Platform — available commands"
	@echo "----------------------------------------"
	@echo "  make setup          First-time setup (copy env, build, start)"
	@echo ""
	@echo "  Stack control"
	@echo "  make up             Start the full stack (detached)"
	@echo "  make down           Stop and remove containers"
	@echo "  make restart        Restart all containers"
	@echo "  make build          Rebuild all Docker images"
	@echo "  make ps             Show container status"
	@echo "  make logs           Tail logs for all services"
	@echo "  make logs-collector Tail collector logs only"
	@echo "  make logs-airflow   Tail Airflow webserver logs"
	@echo ""
	@echo "  Database"
	@echo "  make psql           Open a psql shell into ecommerce DB"
	@echo "  make show-tables    List all staging and analytics tables"
	@echo "  make row-counts     Show row counts for key analytics tables"
	@echo ""
	@echo "  Pipeline"
	@echo "  make trigger        Trigger the ecommerce_pipeline DAG manually"
	@echo "  make dag-list       List all registered Airflow DAGs"
	@echo "  make dag-logs       Stream logs from the last pipeline run"
	@echo ""
	@echo "  Metabase"
	@echo "  make seed-metabase  Seed Metabase dashboards via API"
	@echo ""
	@echo "  Testing"
	@echo "  make test           Run the full unit test suite"
	@echo ""
	@echo "  Cleanup"
	@echo "  make clean          Stop containers and remove images"
	@echo "  make nuke           Full teardown including all volumes (DATA LOSS)"
	@echo ""

# ============================================================
# First-time setup
# ============================================================

setup: .env up
	@echo "Stack is up. Access:"
	@echo "  Airflow  -> http://localhost:8080  (admin / see .env)"
	@echo "  MinIO    -> http://localhost:9001"
	@echo "  Metabase -> http://localhost:3000"

.env:
	@echo "Creating .env from .env.example ..."
	cp .env.example .env
	@echo "Edit .env to set your credentials before continuing."

# ============================================================
# Stack control
# ============================================================

up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose restart

build:
	docker compose build

ps:
	docker compose ps

logs:
	docker compose logs -f

logs-collector:
	docker compose logs -f collector

logs-airflow:
	docker compose logs -f airflow-webserver

# ============================================================
# Database
# ============================================================

psql:
	docker exec -it ecommerce_postgres \
		psql -U ecommerce_user -d ecommerce

show-tables:
	docker exec ecommerce_postgres psql -U ecommerce_user -d ecommerce \
		-c "\dt staging.*" \
		-c "\dt analytics.*"

row-counts:
	docker exec ecommerce_postgres psql -U ecommerce_user -d ecommerce -c " \
		SELECT 'staging.orders'          AS table_name, COUNT(*) FROM staging.orders \
		UNION ALL \
		SELECT 'analytics.fact_orders',              COUNT(*) FROM analytics.fact_orders \
		UNION ALL \
		SELECT 'analytics.dim_customers',            COUNT(*) FROM analytics.dim_customers \
		UNION ALL \
		SELECT 'analytics.dim_products',             COUNT(*) FROM analytics.dim_products \
		UNION ALL \
		SELECT 'analytics.agg_revenue',              COUNT(*) FROM analytics.agg_revenue \
		ORDER BY 1;"

# ============================================================
# Pipeline
# ============================================================

trigger:
	docker exec ecommerce_airflow_webserver \
		airflow dags trigger ecommerce_pipeline

dag-list:
	docker exec ecommerce_airflow_webserver airflow dags list

dag-logs:
	docker exec ecommerce_airflow_webserver \
		airflow tasks logs ecommerce_pipeline transform_to_analytics \
		$$(docker exec ecommerce_airflow_webserver \
			airflow dags list-runs -d ecommerce_pipeline --no-backfill -o plain \
			| awk 'NR==2 {print $$3}')

# ============================================================
# Metabase
# ============================================================

seed-metabase:
	@echo "Seeding Metabase dashboards via Docker ..."
	docker compose run --rm seed-metabase

# ============================================================
# Testing
# ============================================================

test:
	pip install -q -r requirements-dev.txt
	python -m pytest tests/test_validators.py tests/test_cleaners.py -v

# ============================================================
# Cleanup
# ============================================================

clean:
	docker compose down --rmi local

nuke:
	@echo "WARNING: This will delete all data volumes."
	docker compose down -v --rmi local
