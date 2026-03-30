"""
Ingestion: Load Olist CSV files into PostgreSQL raw schema.

Each run is idempotent — tables are replaced on every execution.
All columns are loaded as TEXT; type casting happens in dbt staging models.
"""
import logging
import os

import pandas as pd
from sqlalchemy import create_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# CSV filename → raw table name
DATASETS: dict[str, str] = {
    "orders":                          "olist_orders_dataset.csv",
    "order_items":                     "olist_order_items_dataset.csv",
    "customers":                       "olist_customers_dataset.csv",
    "products":                        "olist_products_dataset.csv",
    "sellers":                         "olist_sellers_dataset.csv",
    "order_payments":                  "olist_order_payments_dataset.csv",
    "order_reviews":                   "olist_order_reviews_dataset.csv",
    "geolocation":                     "olist_geolocation_dataset.csv",
    "product_category_name_translation": "product_category_name_translation.csv",
}


def _get_engine():
    url = (
        f"postgresql+psycopg2://"
        f"{os.environ['OLIST_DB_USER']}:{os.environ['OLIST_DB_PASSWORD']}"
        f"@{os.environ['OLIST_DB_HOST']}:{os.environ.get('OLIST_DB_PORT', '5432')}"
        f"/{os.environ['OLIST_DB_NAME']}"
    )
    return create_engine(url)


def check_files(data_dir: str) -> None:
    """Raise FileNotFoundError if any expected CSV is missing."""
    missing = [
        fname
        for fname in DATASETS.values()
        if not os.path.exists(os.path.join(data_dir, fname))
    ]
    if missing:
        raise FileNotFoundError(
            f"Missing files in {data_dir}:\n" + "\n".join(f"  - {f}" for f in missing)
        )
    logger.info(f"All {len(DATASETS)} CSV files found in {data_dir}")


def _load_table(engine, table: str, path: str) -> int:
    logger.info(f"Loading {os.path.basename(path)} → raw.{table}")
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df = df.replace("", None)  # empty strings become NULL

    df.to_sql(
        table,
        engine,
        schema="raw",
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=5_000,
    )
    logger.info(f"  ✓ {len(df):,} rows → raw.{table}")
    return len(df)


def run(data_dir: str = None) -> None:
    data_dir = data_dir or os.environ.get("DATA_DIR", "/opt/data")
    check_files(data_dir)

    engine = _get_engine()
    total = 0

    for table, filename in DATASETS.items():
        total += _load_table(engine, table, os.path.join(data_dir, filename))

    logger.info(f"Ingestion complete — {total:,} total rows loaded across {len(DATASETS)} tables")


if __name__ == "__main__":
    run()
