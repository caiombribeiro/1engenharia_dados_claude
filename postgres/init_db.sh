#!/bin/bash
# Runs once on first postgres container startup.
# Creates the olist data warehouse and airflow metadata databases.
set -e

echo "Initializing databases..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER olist WITH PASSWORD 'olist';
    CREATE DATABASE olist OWNER olist;

    CREATE USER airflow WITH PASSWORD 'airflow';
    CREATE DATABASE airflow OWNER airflow;
EOSQL

# Create schemas inside the olist database
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "olist" <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE SCHEMA IF NOT EXISTS staging;
    CREATE SCHEMA IF NOT EXISTS marts;

    GRANT ALL ON SCHEMA raw     TO olist;
    GRANT ALL ON SCHEMA staging TO olist;
    GRANT ALL ON SCHEMA marts   TO olist;

    ALTER DEFAULT PRIVILEGES IN SCHEMA raw     GRANT ALL ON TABLES TO olist;
    ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON TABLES TO olist;
    ALTER DEFAULT PRIVILEGES IN SCHEMA marts   GRANT ALL ON TABLES TO olist;
EOSQL

echo "Databases initialized."
