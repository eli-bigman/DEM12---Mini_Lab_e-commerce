-- ============================================================
-- PostgreSQL Initialization Script
-- Runs automatically on first container startup.
-- ============================================================

-- Create a dedicated user for Metabase (avoids using the main app user)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'metabase_user') THEN
        CREATE ROLE metabase_user WITH LOGIN PASSWORD 'metabase_secret_password';
    END IF;
END
$$;

-- Create Metabase's dedicated database
SELECT 'CREATE DATABASE metabase OWNER metabase_user'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase')\gexec

-- Grant necessary privileges on the main ecommerce database
GRANT ALL PRIVILEGES ON DATABASE ecommerce TO ecommerce_user;

-- Connect to the ecommerce database for schema-level grants
\connect ecommerce

-- Create a dedicated analytics schema for star schema tables (Phases 4+)
CREATE SCHEMA IF NOT EXISTS analytics AUTHORIZATION ecommerce_user;

-- Create a staging schema for raw loaded data (Phase 3+)
CREATE SCHEMA IF NOT EXISTS staging AUTHORIZATION ecommerce_user;

-- Enable UUID generation support
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Grant read access to Metabase user on the analytics schema (for Phase 5)
GRANT USAGE ON SCHEMA analytics TO metabase_user;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO metabase_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics
    GRANT SELECT ON TABLES TO metabase_user;
