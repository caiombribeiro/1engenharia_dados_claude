FROM apache/airflow:2.9.2-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install dbt-postgres and ingestion dependencies
# SQLAlchemy is pinned to 1.x for Airflow 2.9 compatibility
USER airflow
RUN pip install --no-cache-dir \
    "dbt-postgres~=1.7.0" \
    "pandas==2.1.4" \
    "psycopg2-binary==2.9.9" \
    "SQLAlchemy>=1.4.0,<2.0"
