#!/bin/bash
set -e

# Default values if not set
POSTGRES_USER=${POSTGRES_USER:-ecommerce_user}
POSTGRES_DB=${POSTGRES_DB:-ecommerce}
MB_DB_USER=${MB_DB_USER:-metabase_user}
MB_DB_PASS=${MB_DB_PASS:-metabase_secret_password}
MB_DB_DBNAME=${MB_DB_DBNAME:-metabase}

echo "Initializing PostgreSQL roles and databases..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Create Metabase User
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${MB_DB_USER}') THEN
            CREATE ROLE ${MB_DB_USER} WITH LOGIN PASSWORD '${MB_DB_PASS}';
        END IF;
    END
    \$\$;

    -- Create Metabase Database
    SELECT 'CREATE DATABASE ${MB_DB_DBNAME} OWNER ${MB_DB_USER}'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${MB_DB_DBNAME}')\gexec

    -- Grant necessary privileges on the main ecommerce database
    GRANT ALL PRIVILEGES ON DATABASE ${POSTGRES_DB} TO ${POSTGRES_USER};
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Create schemas
    CREATE SCHEMA IF NOT EXISTS analytics AUTHORIZATION ${POSTGRES_USER};
    CREATE SCHEMA IF NOT EXISTS staging AUTHORIZATION ${POSTGRES_USER};

    -- Enable UUID generation support
    CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

    -- Grant read access to Metabase user on the analytics schema
    GRANT USAGE ON SCHEMA analytics TO ${MB_DB_USER};
    GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO ${MB_DB_USER};
    ALTER DEFAULT PRIVILEGES IN SCHEMA analytics
        GRANT SELECT ON TABLES TO ${MB_DB_USER};
EOSQL

echo "Done initializing roles and basic permissions."
