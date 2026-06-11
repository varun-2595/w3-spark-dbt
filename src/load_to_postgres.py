import logging

import pandas as pd
from sqlalchemy import create_engine, text

from src.config import RAW_BATCH_1_PATH, ZONE_LOOKUP_PATH

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("LoadToPostgres")

# Columns the dbt staging model (and the cross-system SHA-256 fingerprint)
# expects. Any column missing from the source parquet is added as NULL so the
# fingerprint expression stays identical across Spark and PostgreSQL
# (concat_ws skips NULLs in both engines).
EXPECTED_COLUMNS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance", "RatecodeID", "store_and_fwd_flag",
    "PULocationID", "DOLocationID", "payment_type", "fare_amount", "extra",
    "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
    "total_amount", "congestion_surcharge", "Airport_fee", "driver_name",
]


def load_data():
    # Database connection parameters (host side uses port 5433)
    conn_str = "postgresql://postgres:postgres@localhost:5433/warehouse_db"
    logger.info(f"Connecting to Postgres at {conn_str}...")
    engine = create_engine(conn_str)

    with engine.connect() as conn:
        logger.info("Creating schema 'raw' and pgcrypto extension (for SHA-256 digests)...")
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
        logger.info("Dropping existing raw tables with CASCADE to clean up dependent views...")
        conn.execute(text("DROP TABLE IF EXISTS raw.taxi_zones CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS raw.taxi_trips CASCADE;"))
        logger.info("Dropping historical snapshots schema to ensure clean state...")
        conn.execute(text("DROP SCHEMA IF EXISTS snapshots CASCADE;"))
        conn.commit()

    # 1. Load zones (real TLC zone lookup — see download_tlc_data.py)
    logger.info(f"Reading zones from {ZONE_LOOKUP_PATH}...")
    df_zones = pd.read_csv(ZONE_LOOKUP_PATH)
    logger.info(f"Writing {len(df_zones)} zones to raw.taxi_zones...")
    df_zones.to_sql("taxi_zones", engine, schema="raw", if_exists="replace", index=False)

    # 2. Load trips (real TLC batch 1)
    logger.info(f"Reading trips from {RAW_BATCH_1_PATH}...")
    df_trips = pd.read_parquet(RAW_BATCH_1_PATH)
    for col in EXPECTED_COLUMNS:
        if col not in df_trips.columns:
            logger.info(f"Source has no '{col}' column — adding as NULL for dbt schema stability.")
            df_trips[col] = None
    df_trips = df_trips[EXPECTED_COLUMNS]

    logger.info(f"Writing {len(df_trips):,} trips to raw.taxi_trips (chunked)...")
    df_trips.to_sql(
        "taxi_trips", engine, schema="raw", if_exists="replace",
        index=False, chunksize=50_000, method="multi",
    )

    logger.info("Database loading completed successfully!")


if __name__ == "__main__":
    load_data()
