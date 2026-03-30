"""
DAG: olist_etl_pipeline
Schedule: daily

Task chain:
  check_files → ingest_raw → dbt_deps → dbt_staging → dbt_marts → dbt_test
"""
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DBT_DIR = "/opt/dbt"
DBT_FLAGS = f"--profiles-dir {DBT_DIR} --project-dir {DBT_DIR} --no-use-colors"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="olist_etl_pipeline",
    default_args=default_args,
    description="Olist CSVs → PostgreSQL raw → dbt staging → dbt marts",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["olist", "etl", "e-commerce", "dbt"],
) as dag:

    # ── Task 1: verify all CSV files are present ──────────────────────────
    def _check_files():
        sys.path.insert(0, "/opt/ingestion")
        from ingest import check_files
        check_files(os.environ.get("DATA_DIR", "/opt/data"))

    check_files_task = PythonOperator(
        task_id="check_files",
        python_callable=_check_files,
    )

    # ── Task 2: load CSVs into raw schema ────────────────────────────────
    def _ingest_raw():
        sys.path.insert(0, "/opt/ingestion")
        from ingest import run
        run()

    ingest_raw = PythonOperator(
        task_id="ingest_raw",
        python_callable=_ingest_raw,
    )

    # ── Task 3: install dbt packages ─────────────────────────────────────
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"dbt deps {DBT_FLAGS}",
    )

    # ── Task 4: build staging views ──────────────────────────────────────
    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=f"dbt run --select staging {DBT_FLAGS}",
    )

    # ── Task 5: build mart tables ────────────────────────────────────────
    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command=f"dbt run --select marts {DBT_FLAGS}",
    )

    # ── Task 6: run all dbt tests ────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test {DBT_FLAGS}",
    )

    # ── Dependencies ─────────────────────────────────────────────────────
    check_files_task >> ingest_raw >> dbt_deps >> dbt_staging >> dbt_marts >> dbt_test
