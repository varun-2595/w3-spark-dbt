import os
import subprocess
import logging
from typing import Tuple, Optional
import pandas as pd
from sqlalchemy import create_engine, text

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TestSCD2")


class DatabaseManager:
    """Manages the connection, queries, and updates to the PostgreSQL database."""

    def __init__(self, conn_str: str = "postgresql://postgres:postgres@localhost:5433/warehouse_db"):
        self.conn_str = conn_str
        self.engine = create_engine(conn_str)

    def update_location_zone(self, location_id: int, new_zone_name: str) -> None:
        """Updates the zone name for a specific LocationID in the raw source table."""
        logger.info(f"Updating LocationID = {location_id} in raw.taxi_zones to '{new_zone_name}'...")
        query = text("""
            UPDATE raw.taxi_zones 
            SET "Zone" = :zone_name 
            WHERE "LocationID" = :loc_id;
        """)
        with self.engine.connect() as conn:
            conn.execute(query, {"zone_name": new_zone_name, "loc_id": location_id})
            conn.commit()

    def get_raw_location_state(self, location_id: int) -> Tuple[int, str]:
        """Queries and returns the current state of LocationID and Zone in raw.taxi_zones."""
        query = text('SELECT "LocationID", "Zone" FROM raw.taxi_zones WHERE "LocationID" = :loc_id;')
        with self.engine.connect() as conn:
            row = conn.execute(query, {"loc_id": location_id}).fetchone()
            if row is None:
                raise ValueError(f"No record found in raw.taxi_zones for LocationID = {location_id}")
            return int(row[0]), str(row[1])

    def fetch_location_snapshots(self, location_id: int) -> pd.DataFrame:
        """Queries the snapshot history table snapshots.location_snapshot for a specific location_id."""
        query = text("""
            SELECT 
                location_id, 
                borough, 
                zone_name, 
                service_zone, 
                dbt_valid_from, 
                dbt_valid_to 
            FROM snapshots.location_snapshot 
            WHERE location_id = :loc_id 
            ORDER BY dbt_valid_from;
        """)
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"loc_id": location_id})
            return df


class DbtContainerRunner:
    """Invokes DBT commands inside a Docker container mapped to the local project folder and network."""

    def __init__(self, dbt_project_dir: str, network_name: str = "w3-spark-dbt_default", 
                 dbt_image: str = "ghcr.io/dbt-labs/dbt-postgres:1.7.3"):
        self.dbt_project_dir = dbt_project_dir
        self.network_name = network_name
        self.dbt_image = dbt_image

    def execute_command(self, command: str) -> None:
        """Runs a dbt subcommand (e.g. 'run', 'snapshot') inside the dbt container."""
        logger.info(f"Invoking 'dbt {command}' inside the dbt-postgres container...")
        
        # Construct docker command with volume mapping to local dbt project files
        run_cmd = [
            "docker", "run", "--rm", 
            "--network", self.network_name,
            "-v", f"{self.dbt_project_dir}:/usr/app/dbt",
            self.dbt_image, command, 
            "--profiles-dir", "/usr/app/dbt"
        ]
        
        result = subprocess.run(run_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"DBT command failed (Exit Code {result.returncode})!")
            logger.error(f"STDOUT:\n{result.stdout}")
            logger.error(f"STDERR:\n{result.stderr}")
            raise RuntimeError(f"dbt command '{command}' failed.")
        
        logger.info(f"DBT command '{command}' completed successfully.")
        # Log summary of output
        lines = result.stdout.splitlines()
        for line in lines[-10:]:  # Show last 10 lines of dbt log for summary
            logger.info(f"[DBT OUTPUT] {line}")


class Scd2Tester:
    """Orchestrates the verification of Slowing Changing Dimension (SCD) Type 2 logic using dbt snapshots."""

    def __init__(self, db_manager: DatabaseManager, dbt_runner: DbtContainerRunner, target_loc_id: int = 5):
        self.db_manager = db_manager
        self.dbt_runner = dbt_runner
        self.target_loc_id = target_loc_id

    def verify_scd2(self) -> bool:
        """Executes full SCD Type 2 update, dbt run, dbt snapshot, and history verification flow."""
        logger.info("============================================================")
        logger.info("STARTING SCD TYPE 2 VERIFICATION TEST")
        logger.info("============================================================")

        try:
            # 1. Capture baseline state (Clean database initial run)
            logger.info("Step 1: Building baseline dimension models and snapshots...")
            self.dbt_runner.execute_command("run")
            self.dbt_runner.execute_command("snapshot")

            # 2. Update source table
            new_zone = "SI Zone 5 Updated"
            logger.info(f"Step 2: Updating LocationID = {self.target_loc_id} in raw.taxi_zones to '{new_zone}'...")
            self.db_manager.update_location_zone(self.target_loc_id, new_zone)
            
            # Verify raw update has taken place
            loc_id, zone_name = self.db_manager.get_raw_location_state(self.target_loc_id)
            logger.info(f"Verified raw source state: LocationID={loc_id}, Zone='{zone_name}'")

            # 3. Run dbt run to update dimension tables
            logger.info("Step 3: Triggering DBT models build stage for updated state...")
            self.dbt_runner.execute_command("run")

            # 4. Run dbt snapshot to capture the history update
            logger.info("Step 4: Triggering DBT snapshots stage to record the change...")
            self.dbt_runner.execute_command("snapshot")

            # 5. Fetch snapshot history and assert SCD Type 2 versioning
            logger.info("Step 5: Analyzing snapshots table to verify SCD Type 2 history...")
            df_snap = self.db_manager.fetch_location_snapshots(self.target_loc_id)

            logger.info("\n=== SCD Type 2 Snapshot Table (Location ID = {}) ===".format(self.target_loc_id))
            print(df_snap.to_string(index=False))
            logger.info("====================================================")

            if len(df_snap) > 1:
                logger.info("✅ SUCCESS: SCD Type 2 history successfully captured!")
                logger.info(f"Retired version active to: {df_snap.iloc[0]['dbt_valid_to']}")
                logger.info(f"Current version active from: {df_snap.iloc[1]['dbt_valid_from']} (valid_to: {df_snap.iloc[1]['dbt_valid_to']})")
                return True
            else:
                logger.error("❌ FAILURE: SCD Type 2 did not capture the update!")
                return False

        except Exception as e:
            logger.error(f"❌ SCD Type 2 Testing encountered an exception: {str(e)}", exc_info=True)
            return False


if __name__ == "__main__":
    local_project_dir = os.path.join(os.getcwd(), 'dbt_project')
    
    db_mgr = DatabaseManager()
    dbt_run = DbtContainerRunner(dbt_project_dir=local_project_dir)
    
    tester = Scd2Tester(db_manager=db_mgr, dbt_runner=dbt_run)
    tester.verify_scd2()
