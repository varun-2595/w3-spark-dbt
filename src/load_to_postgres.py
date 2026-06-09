import os
import pandas as pd
from sqlalchemy import create_engine, text
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("LoadToPostgres")

def load_data():
    # Database connection parameters (host side uses port 5433)
    conn_str = "postgresql://postgres:postgres@localhost:5433/warehouse_db"
    logger.info(f"Connecting to Postgres at {conn_str}...")
    engine = create_engine(conn_str)
    
    # Create raw schema
    with engine.connect() as conn:
        logger.info("Creating schema 'raw' if not exists...")
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
        logger.info("Dropping existing raw tables with CASCADE to clean up dependent views...")
        conn.execute(text("DROP TABLE IF EXISTS raw.taxi_zones CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS raw.taxi_trips CASCADE;"))
        logger.info("Dropping historical snapshots schema to ensure clean state...")
        conn.execute(text("DROP SCHEMA IF EXISTS snapshots CASCADE;"))
        conn.commit()
        
    # Paths
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    trips_path = os.path.join(base_dir, "data", "yellow_tripdata_2026-01.parquet")
    zones_path = os.path.join(base_dir, "data", "taxi_zone_lookup.csv")
    
    # 1. Load zones
    logger.info(f"Reading zones from {zones_path}...")
    df_zones = pd.read_csv(zones_path)
    logger.info(f"Writing {len(df_zones)} zones to raw.taxi_zones...")
    df_zones.to_sql("taxi_zones", engine, schema="raw", if_exists="replace", index=False)
    
    # 2. Load trips (batch 1)
    logger.info(f"Reading trips from {trips_path}...")
    df_trips = pd.read_parquet(trips_path)
    logger.info(f"Writing {len(df_trips)} trips to raw.taxi_trips...")
    df_trips.to_sql("taxi_trips", engine, schema="raw", if_exists="replace", index=False)
    
    logger.info("Database loading completed successfully!")

if __name__ == "__main__":
    load_data()
