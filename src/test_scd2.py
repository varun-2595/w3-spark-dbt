import os
import subprocess
from sqlalchemy import create_engine, text
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TestSCD2")

def test_scd2():
    # Connect to Postgres
    conn_str = "postgresql://postgres:postgres@localhost:5433/warehouse_db"
    engine = create_engine(conn_str)
    
    # 1. Update source table (LocationID = 5)
    logger.info("Step 1: Updating LocationID = 5 in raw.taxi_zones to 'SI Zone 5 Updated'...")
    with engine.connect() as conn:
        conn.execute(text("""
            UPDATE raw.taxi_zones 
            SET "Zone" = 'SI Zone 5 Updated' 
            WHERE "LocationID" = 5;
        """))
        conn.commit()
        
    # Verify raw update
    with engine.connect() as conn:
        res = conn.execute(text('SELECT "LocationID", "Zone" FROM raw.taxi_zones WHERE "LocationID" = 5;')).fetchone()
        logger.info(f"Raw source state: LocationID={res[0]}, Zone='{res[1]}'")

    # 2. Run dbt run to update the dimension table
    logger.info("Step 2: Running 'dbt run' in container...")
    run_cmd = [
        "docker", "run", "--rm", "--network", "w3-spark-dbt_default",
        "-v", f"{os.path.join(os.getcwd(), 'dbt_project')}:/usr/app/dbt",
        "ghcr.io/dbt-labs/dbt-postgres:1.7.3", "run", "--profiles-dir", "/usr/app/dbt"
    ]
    subprocess.run(run_cmd, check=True)

    # 3. Run dbt snapshot to capture the change
    logger.info("Step 3: Running 'dbt snapshot' in container...")
    snap_cmd = [
        "docker", "run", "--rm", "--network", "w3-spark-dbt_default",
        "-v", f"{os.path.join(os.getcwd(), 'dbt_project')}:/usr/app/dbt",
        "ghcr.io/dbt-labs/dbt-postgres:1.7.3", "snapshot", "--profiles-dir", "/usr/app/dbt"
    ]
    subprocess.run(snap_cmd, check=True)

    # 4. Query snapshot results and print
    logger.info("Step 4: Querying snapshots.location_snapshot for location_id = 5...")
    with engine.connect() as conn:
        df_snap = pd.read_sql(text("""
            SELECT 
                location_id, 
                borough, 
                zone_name, 
                service_zone, 
                dbt_valid_from, 
                dbt_valid_to 
            FROM snapshots.location_snapshot 
            WHERE location_id = 5 
            ORDER BY dbt_valid_from;
        """), conn)
        
        logger.info("\n=== SCD Type 2 Snapshot Table (Location ID = 5) ===")
        print(df_snap.to_string(index=False))
        logger.info("====================================================")
        
        if len(df_snap) > 1:
            logger.info("✅ SUCCESS: SCD Type 2 history successfully captured!")
            logger.info(f"Old record retired at: {df_snap.iloc[0]['dbt_valid_to']}")
            logger.info(f"New record active from: {df_snap.iloc[1]['dbt_valid_from']} (valid_to is {df_snap.iloc[1]['dbt_valid_to']})")
        else:
            logger.error("❌ FAILURE: SCD Type 2 did not capture the update!")

if __name__ == "__main__":
    test_scd2()
