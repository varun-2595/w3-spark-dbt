# Evidence Artifacts — Week 3 Spark Pipeline

Every file in this folder except the Spark UI screenshots is generated
**automatically and verbatim** by `src/pyspark_pipeline.py` on each run:

| File | Checkpoint covered |
|---|---|
| `01_data_profile.txt` | Partition count, partition-level data skew, schema |
| `02_plan_comparison_df_vs_sql.txt` | DataFrame API vs Spark SQL EXPLAIN plans, documented comparison |
| `03_row_level_diff.txt` | DataFrame and SQL produce identical output (row-level `exceptAll` diff) |
| `04_window_spot_check.txt` | Top-ranked trip per zone matches manual spot-check; rolling 7-day recount |
| `05_explain_broadcast_join.txt` | Verbatim EXPLAIN: `BroadcastHashJoin` vs `SortMergeJoin` |
| `06_shuffle_tuning_timings.txt` | Raw per-run timings, 200 vs 8 shuffle partitions, % improvement |
| `07_iceberg_metadata.txt` | Iceberg Silver/Gold snapshots, history, and data files |
| `spark_ui_dag_annotated.png` | Spark UI DAG screenshot (captured manually — see below) |

## Capturing the Spark UI DAG screenshots

1. Start the job with the UI held open after completion:
   ```bash
   docker exec -u root -e KEEP_UI_SECONDS=600 spark-master /opt/bitnami/spark/bin/spark-submit \
     --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.iceberg:iceberg-aws-bundle:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4 \
     /opt/spark/workspace/src/pyspark_pipeline.py
   ```
2. While it runs (or during the 10-minute hold), open:
   - **http://localhost:4040** — driver UI (port is now mapped in docker-compose).
   - **http://localhost:8080** — master UI showing the app on **2 workers** (screenshot this too for the cluster checkpoint).
3. In the driver UI go to **Jobs → (largest job) → DAG Visualization**. The most
   complex job is the Stage 3 row-level diff (two full pipelines + `exceptAll`)
   or the Stage 7 Iceberg write.
4. Screenshot the DAG, then annotate (arrows/labels for: parquet scan, Filter,
   the two `Exchange hashpartitioning` shuffles, HashAggregate vs Window path,
   BroadcastExchange on the zone lookup) and save here as
   `spark_ui_dag_annotated.png`.
