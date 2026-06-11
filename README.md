# NYC Yellow Taxi Lakehouse & Star Schema Data Warehouse (Week 3)

This project processes the **real NYC TLC Yellow Taxi dataset** (~6M rows across two months) through two components:

1. **Distributed Processing with PySpark** — cleaning, deduplication, window functions, broadcast joins, and shuffle tuning on a standalone Spark cluster (1 master + 2 workers), writing Silver/Gold tables to Apache Iceberg on MinIO.
2. **Data Warehouse with dbt** — a star schema in PostgreSQL with schema tests and SCD Type 2 snapshots.

Every pipeline run automatically writes verbatim verification evidence (EXPLAIN plans, row-level diffs, raw timings, Iceberg metadata) to `docs/evidence/`.

---

## Architecture

```
                  [ NYC TLC CloudFront CDN ]
                             │
                  (download_tlc_data.py)
                             │
                             ▼
          [ REAL TLC Parquet + zone lookup CSV ]
                             │
         ┌───────────────────┴───────────────────┐
         ▼                                       ▼
 [ PySpark Medallion Pipeline ]        [ dbt Data Warehouse ]
   (Spark Master + 2 Workers)               (PostgreSQL)
         │                                       │
         ▼ (format: iceberg)                     ▼
 [ Iceberg REST Catalog + MinIO S3 ]   [ Star Schema Models ]
 (Silver & Gold tables in warehouse)  (Fact & Dimension tables)
                                                 │
                                                 ▼ (SCD Type 2)
                                       [ snapshots.location_snapshot ]
```

### Docker Services

| Service | Ports | Role |
|---|---|---|
| `w3_postgres` | 5433 | Raw source tables, dbt warehouse, Iceberg catalog backing store |
| `w3_minio` | 9000 / 9001 | S3-compatible object storage for the Iceberg warehouse |
| `w3_rest_catalog` | 8181 | Iceberg REST catalog (JDBC-backed by PostgreSQL) |
| `spark-master` | 8080 / 7077 / 4040 | Standalone Spark master, `bitnamilegacy/spark:3.5.3` (8080 = master UI, 4040 = driver UI) |
| `spark-worker-1/2` | — | Spark workers connected to the master |

`spark-defaults.conf` sets `spark.master spark://spark-master:7077`, so submitted jobs register with the cluster and are visible in the master UI.

---

## Setup & Execution

### 0. Python environment
```bash
python -m venv .venv
.venv\Scripts\activate        # Windows  (Linux/macOS: source .venv/bin/activate)
pip install -r requirements.txt
```

### 1. Start the Docker services
```bash
docker-compose up -d
```

### 2. Download REAL NYC TLC data and load sources
Downloads the real Yellow Taxi Parquet files (2024-01, 2024-02 — ~3M rows each) and the official taxi zone lookup CSV:
```bash
python download_tlc_data.py

# Initialize MinIO bucket & upload the zone lookup CSV
python create_bucket.py
python upload_lookup.py

# Ingest raw source data into PostgreSQL (for the dbt warehouse)
python -m src.load_to_postgres
```

### 3. Run the PySpark pipeline
`KEEP_UI_SECONDS` holds the SparkSession open after completion so the Spark UI DAG (http://localhost:4040) can be screenshotted:
```bash
docker exec -u root -e KEEP_UI_SECONDS=600 spark-master /opt/bitnami/spark/bin/spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.iceberg:iceberg-aws-bundle:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark/workspace/src/pyspark_pipeline.py
```
While it runs: http://localhost:8080 shows the application on 2 workers; http://localhost:4040 shows jobs, stages, and DAG visualizations.

> **Image note:** the assignment prescribes the Bitnami Spark image. Bitnami discontinued its Docker Hub catalog in 2025 and `bitnami/spark` serves no tags anymore; `bitnamilegacy/spark` is Bitnami's official frozen archive of the same images, pinned here to `3.5.3`.

> **Snapshot history:** run the pipeline twice — the second run performs an Iceberg `overwrite` that appends a new snapshot, and `07_iceberg_metadata.txt` will show the multi-snapshot lineage.

### 4. Build and test the dbt warehouse
```bash
cd dbt_project
dbt run
dbt test
```

---

## Evidence Artifacts (`docs/evidence/`)

Each pipeline run regenerates verbatim evidence per checkpoint:

| File | Contents |
|---|---|
| `01_data_profile.txt` | Row counts, partition count, partition-level skew, schema |
| `02_plan_comparison_df_vs_sql.txt` | DataFrame API vs Spark SQL EXPLAIN plans + comparison notes |
| `03_row_level_diff.txt` | Two-sided `exceptAll` row-level diff (outputs identical) |
| `04_window_spot_check.txt` | Manual spot-checks of rank and rolling-window results |
| `05_explain_broadcast_join.txt` | Verbatim `BroadcastHashJoin` vs `SortMergeJoin` plans |
| `06_shuffle_tuning_timings.txt` | Raw per-run timings, default vs tuned partitions, % improvement |
| `07_iceberg_metadata.txt` | Iceberg snapshots, history, and data files after the write |

See `docs/evidence/README.md` for the Spark UI screenshot procedure.

## Key Spark Implementation Notes

- **Real data at scale**: ingests the actual TLC monthly Parquet files; partition-level skew measured via `spark_partition_id()` distribution.
- **Deduplication key**: `trip_fingerprint` is a SHA-256 hash over **all normalized business columns**. dbt computes the identical expression in PostgreSQL via pgcrypto, so fingerprints join across systems.
- **DataFrame ↔ SQL parity**: outputs verified row-level identical via two-sided `exceptAll` (a `60.0` vs `60.0D` DECIMAL-literal bug was caught this way).
- **Window functions**: fare rank per zone-hour (`dense_rank`) and rolling 7-day volume per zone (range frame over `unix_timestamp` seconds — whole-second units regardless of the parquet's microsecond precision), both spot-checked against independent queries.
- **Broadcast join**: zone lookup broadcast to all executors; `BroadcastHashJoin` confirmed in the captured plan AND wall-clock timed against a forced `SortMergeJoin` (warmups discarded).
- **Shuffle tuning + coalesce**: default 200 partitions vs cluster parallelism, benchmarked with AQE disabled against a `noop` sink (warmup runs discarded); plus a `coalesce(2)` output-write demonstration with file counts and timings.
- **Iceberg writes**: Silver + two Gold tables (fare ranking AND rolling 7-day volume) — first run `create()`s, later runs `overwrite()` so snapshot history is preserved across runs; file layout via `write.target-file-size-bytes` / `write.distribution-mode` table properties (file layout governed by Iceberg, not `coalesce`); snapshot metadata captured after each write.

---

## dbt Star Schema & SCD Type 2 Snapshots

Star schema in PostgreSQL — fact table `fact_trips` keyed by the SHA-256 `trip_fingerprint`; dimensions `dim_location`, `dim_date`, `dim_rate_code`, `dim_payment_type`. Schema tests cover uniqueness, not-null, and referential integrity.

### SCD Type 2 simulation
```bash
python -m src.test_scd2
```
The script updates the `Zone` name of LocationID 5 in the raw source, reruns `dbt run` + `dbt snapshot`, and prints the