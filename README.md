# NYC Yellow Taxi Lakehouse & Star Schema Data Warehouse (Week 3)

This project demonstrates a production-grade distributed data engineering workflow. It is built in two primary components:
1. **Distributed Processing with PySpark**: Cleaning, ranking, joins, and shuffle partition tuning over NYC Yellow Taxi datasets in a standalone Spark cluster, writing output to Apache Iceberg.
2. **Modern Data Warehouse with dbt**: Building a star schema in PostgreSQL, orchestrating transformations, and tracking historical changes using Slowly Changing Dimension (SCD) Type 2 snapshots.

---

## 🏗️ Architecture Layout

```
                  [ NYC TLC CloudFront CDN ]
                                 │
                      (download_tlc_data.py)
                                 │
                                 ▼
              [ REAL TLC Parquet + zone lookup CSV ]
                                 │
         ┌───────────────────────┴───────────────────────┐
         ▼                                               ▼
 [ PySpark Medallion Pipeline ]                [ dbt Data Warehouse ]
   (Spark Master + 2 Workers)                       (PostgreSQL)
         │                                               │
         ▼ (write format: iceberg)                       ▼
 [ Iceberg REST Catalog + MinIO S3 ]           [ Star Schema Models ]
 (Silver & Gold Tables in warehouse)          (Fact & Dimension tables)
                                                         │
                                                         ▼ (SCD Type 2)
                                               [ snapshots.location_snapshot ]
```

### Docker Services Map:
- **`w3_postgres` (Port 5433)**: Stores raw source tables and the dbt star schema warehouse.
- **`w3_minio` (Ports 9000 & 9001)**: Simulated S3 storage containing the Iceberg catalog warehouse.
- **`w3_rest_catalog` (Port 8181)**: Iceberg REST catalog backed by SQLite.
- **`spark-master` (Ports 8080 & 7077)**: Standalone Spark master.
- **`spark-worker-1` / `spark-worker-2`**: Spark workers connected to the master.

---

## ⚡ Setup & Execution Instructions

### 1. Start the Docker Services
Ensure Docker Desktop is running, then spin up the infrastructure:
```bash
docker-compose up -d
```

### 2. Download REAL NYC TLC Data and Load Sources
Download the real NYC TLC Yellow Taxi Parquet files (2024-01, 2024-02 — ~3M rows each)
and the official taxi zone lookup CSV:
```bash
python download_tlc_data.py

# Initialize MinIO bucket & upload lookup CSV
python create_bucket.py
python upload_lookup.py

# Ingest raw source data into PostgreSQL (for the dbt warehouse)
python -m src.load_to_postgres
```
> `src/generator.py` now only produces clearly-named `synthetic_tripdata_*` fixtures
> for the SCD2 simulation test — it is **not** a pipeline data source.

### 3. Run the PySpark Pipeline
Submit the medallion processing job to the standalone Spark cluster. `KEEP_UI_SECONDS`
holds the SparkSession open after completion so the Spark UI DAG (http://localhost:4040)
can be screenshotted:
```bash
docker exec -u root -e KEEP_UI_SECONDS=600 spark-master /opt/spark/bin/spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark/workspace/src/pyspark_pipeline.py
```

#### Evidence artifacts (written automatically to `docs/evidence/`)
Every run regenerates verbatim evidence for each checkpoint — data profile and
partition skew, the DataFrame-vs-SQL EXPLAIN plan comparison, the row-level
`exceptAll` diff, window-function spot-checks, broadcast vs sort-merge EXPLAIN
output, raw shuffle-tuning timings with % improvement, and Iceberg
snapshot/history/files metadata. See `docs/evidence/README.md` for the file map
and the Spark UI screenshot procedure.

#### Key Spark Highlights:
- **Real data at scale**: Ingests real TLC monthly Parquet (multi-million rows), profiled for partition-level skew.
- **Cleaning & Deduplication**: Validates fares/durations/zones and deduplicates by a `SHA-256` trip fingerprint computed over **all normalized business columns** — the same expression dbt uses in PostgreSQL (pgcrypto), so fingerprints join across systems.
- **Spark SQL Parity**: DataFrame API and Spark SQL outputs verified **row-level identical** via two-sided `exceptAll` diff; execution plans captured and compared.
- **Window Functions**: Ranks top fares per zone-hour and computes rolling 7-day volume per zone (range frame over `unix_timestamp` seconds), both spot-checked against independent queries.
- **Optimized Join**: `BroadcastHashJoin` vs `SortMergeJoin` plans captured verbatim in evidence.
- **Shuffle Tuning**: 200 vs 8 partitions benchmarked with AQE disabled, raw per-run timings recorded.
- **Iceberg Silver + Gold**: Written via `writeTo(...).createOrReplace()` with `write.target-file-size-bytes` / `write.distribution-mode` table properties (file layout governed by Iceberg, not `coalesce`); snapshot metadata captured as evidence.

---

## 📊 dbt Star Schema & SCD Type 2 Snapshots

The data warehouse inside PostgreSQL is organized into a star schema:
- **Fact Table**: `fact_trips`
- **Dimension Tables**: `dim_location`, `dim_date`, `dim_rate_code`, `dim_payment_type`

### Run SCD Type 2 Snapshots Test
We track historical changes to the location zones dimension (`dim_location` checking `borough`, `zone_name`, and `service_zone`) using `dbt snapshot`.

Run the automated simulation test:
```bash
python -m src.test_scd2
```
This script will:
1. Update Location ID `5` from `SI Zone 5` to `SI Zone 5 Updated` in the source database.
2. Run `dbt run` to refresh the dimensions.
3. Run `dbt snapshot` to capture and evolve the state.
4. Output the query results showing the old record successfully retired (with a valid end timestamp) and the new record created (with `dbt_valid_to` as `NULL`).

---

## 📁 Repository Structure
- `src/`: PySpark pipeline, configuration, data generation, and load utilities.
- `dbt_project/`: Full dbt models, profiles, snapshots, and sources directory.
- `docker-compose.yml`: Local infrastructure services definitions.
- `spark-defaults.conf`: Apache Spark session configurations mapping REST catalog and MinIO S3 credentials.
