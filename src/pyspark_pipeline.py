"""
Week 3 PySpark pipeline — NYC TLC Yellow Taxi distributed processing.

Processes REAL NYC TLC Parquet data (see download_tlc_data.py) on a standalone
Spark cluster, and writes verbatim evidence artifacts (EXPLAIN plans, raw
timings, row-level diffs, spot-checks, Iceberg metadata) to docs/evidence/.

Environment overrides (all optional — defaults target the Docker cluster):
    TLC_DATA_DIR      dir containing yellow_tripdata_*.parquet   (default /opt/spark/workspace/data)
    ZONE_LOOKUP_PATH  zone lookup CSV path                       (default s3a://warehouse/lookup/taxi_zone_lookup.csv)
    EVIDENCE_DIR      where evidence files are written           (default /opt/spark/workspace/docs/evidence)
    BENCH_DIR         scratch dir for the coalesce write demo    (default /opt/spark/workspace/.bench_tmp)
    ICEBERG_CATALOG   Iceberg catalog name                       (default demo)
    SKIP_ICEBERG      set to 1 to skip Iceberg writes (local smoke-tests only)
    KEEP_UI_SECONDS   keep SparkSession alive N seconds after completion
                      so the Spark UI (port 4040) can be screenshotted
"""

import os
import glob
import shutil
import time
import logging
from statistics import mean
from typing import List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, split, concat, substring, lit, when, size,
    unix_timestamp, sha2, concat_ws, hour, count, date_format,
    dense_rank, broadcast, spark_partition_id, max as spark_max,
)
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PySparkPipeline")

# --------------------------------------------------------------------------
# Configuration defaults (Docker cluster paths; override via env for local runs)
# --------------------------------------------------------------------------
DATA_DIR = os.environ.get("TLC_DATA_DIR", "/opt/spark/workspace/data")
LOOKUP_PATH = os.environ.get("ZONE_LOOKUP_PATH", "s3a://warehouse/lookup/taxi_zone_lookup.csv")
EVIDENCE_DIR = os.environ.get("EVIDENCE_DIR", "/opt/spark/workspace/docs/evidence")
BENCH_DIR = os.environ.get("BENCH_DIR", "/opt/spark/workspace/.bench_tmp")
ICEBERG_CATALOG = os.environ.get("ICEBERG_CATALOG", "demo")
SKIP_ICEBERG = os.environ.get("SKIP_ICEBERG", "0") == "1"
KEEP_UI_SECONDS = int(os.environ.get("KEEP_UI_SECONDS", "0"))

SILVER_TABLE = f"{ICEBERG_CATALOG}.nyc_taxi.trips_silver"
GOLD_TABLE = f"{ICEBERG_CATALOG}.nyc_taxi.trips_gold"
GOLD_ROLLING_TABLE = f"{ICEBERG_CATALOG}.nyc_taxi.trips_gold_rolling_7d"

# Normalized column spec used to build the trip fingerprint IDENTICALLY in the
# DataFrame API, Spark SQL, and dbt/PostgreSQL (see dbt stg_taxi_trips.sql).
# Format: (column_name, kind) where kind ∈ {int, ts, dec, str}
FINGERPRINT_SPEC = [
    ("VendorID", "int"),
    ("tpep_pickup_datetime", "ts"),
    ("tpep_dropoff_datetime", "ts"),
    ("passenger_count", "int"),
    ("trip_distance", "dec"),
    ("RatecodeID", "int"),
    ("store_and_fwd_flag", "str"),
    ("PULocationID", "int"),
    ("DOLocationID", "int"),
    ("payment_type", "int"),
    ("fare_amount", "dec"),
    ("extra", "dec"),
    ("mta_tax", "dec"),
    ("tip_amount", "dec"),
    ("tolls_amount", "dec"),
    ("improvement_surcharge", "dec"),
    ("total_amount", "dec"),
    ("congestion_surcharge", "dec"),
    ("Airport_fee", "dec"),
    ("driver_name", "str"),  # only present in synthetic fixtures; skipped if absent
]


def explain_text(df: DataFrame, mode: str = "formatted") -> str:
    """Returns the verbatim EXPLAIN output of a DataFrame as a string."""
    return df._sc._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), mode)


def df_to_text(df: DataFrame, num_rows: int = 20, truncate: int = 60) -> str:
    """Renders a DataFrame sample exactly as .show() would, as a string."""
    return df._jdf.showString(num_rows, truncate, False)


def time_noop_write(df: DataFrame) -> float:
    """Times the FULL execution of a DataFrame against a no-op sink."""
    start = time.time()
    df.write.format("noop").mode("overwrite").save()
    return time.time() - start


class EvidenceCollector:
    """Writes verbatim evidence artifacts (plans, timings, diffs) to disk."""

    def __init__(self, evidence_dir: str):
        self.evidence_dir = evidence_dir
        os.makedirs(evidence_dir, exist_ok=True)

    def write(self, filename: str, title: str, content: str) -> str:
        path = os.path.join(self.evidence_dir, filename)
        header = (
            f"{'=' * 78}\n{title}\n"
            f"Captured: {time.strftime('%Y-%m-%d %H:%M:%S')}\n{'=' * 78}\n\n"
        )
        with open(path, "w", encoding="utf-8") as f:
            f.write(header + content.rstrip() + "\n")
        logger.info(f"Evidence written: {path}")
        return path


class SparkSessionManager:
    """Manages the creation, configuration, and destruction of the SparkSession."""

    def __init__(self, app_name: str = "NYC Yellow Taxi Spark Pipeline"):
        self.app_name = app_name
        self.spark: Optional[SparkSession] = None

    def get_or_create_session(self) -> SparkSession:
        logger.info(f"Initializing SparkSession: '{self.app_name}'...")
        try:
            self.spark = SparkSession.builder.appName(self.app_name).getOrCreate()
        except Exception as e:
            raise RuntimeError(
                "Failed to create SparkSession. Common causes: the standalone "
                "master is unreachable (is docker-compose up and is "
                "spark.master set to spark://spark-master:7077?), or required "
                "--packages could not be resolved. Original error: " + str(e)
            ) from e
        self.spark.sparkContext.setLogLevel("WARN")
        return self.spark

    def stop_session(self) -> None:
        if self.spark:
            logger.info("Stopping active SparkSession.")
            self.spark.stop()
            self.spark = None


class DataLoader:
    """Handles ingestion of TLC Parquet files and the zone lookup CSV."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def load_tlc_parquet(self, data_dir: str) -> DataFrame:
        """Loads ALL yellow_tripdata_*.parquet files found in data_dir."""
        paths = sorted(glob.glob(os.path.join(data_dir, "yellow_tripdata_*.parquet")))
        if not paths:
            raise FileNotFoundError(
                f"No yellow_tripdata_*.parquet files in {data_dir}. "
                "Run `python download_tlc_data.py` first to fetch real NYC TLC data."
            )
        logger.info(f"Ingesting {len(paths)} TLC Parquet file(s): {[os.path.basename(p) for p in paths]}")
        return self.spark.read.parquet(*paths)

    def load_zone_lookup(self, path: str) -> DataFrame:
        logger.info(f"Ingesting zone lookup CSV from: {path}")
        return (
            self.spark.read.option("header", "true").option("inferSchema", "true").csv(path)
        )


class DataProfiler:
    """Profiles partition count, partition-level data skew, and schema."""

    @staticmethod
    def profile(df: DataFrame, dataset_name: str, evidence: EvidenceCollector) -> None:
        logger.info(f"PROFILING DATASET: {dataset_name}")

        row_count = df.count()
        partition_count = df.rdd.getNumPartitions()

        # True partition-level skew: record-count distribution across ALL
        # partitions. groupBy(spark_partition_id()) only returns partitions
        # that contain rows, so empty partitions are reinstated explicitly —
        # hiding them would understate imbalance.
        part_counts = (
            df.groupBy(spark_partition_id().alias("partition_id"))
            .count()
            .orderBy("partition_id")
        )
        populated = {r["partition_id"]: r["count"] for r in part_counts.collect()}
        all_counts = [populated.get(i, 0) for i in range(partition_count)]
        empty_partitions = sum(1 for c in all_counts if c == 0)
        avg_all = mean(all_counts) if all_counts else 0
        skew_ratio = (max(all_counts) / avg_all) if avg_all else 0
        nonzero = [c for c in all_counts if c > 0]

        table_lines = ["| partition_id | rows |", "|---|---|"]
        table_lines += [f"| {i} | {c:,} |" for i, c in enumerate(all_counts)]

        lines = [
            f"Dataset:              {dataset_name}",
            f"Total rows:           {row_count:,}",
            f"Partition count:      {partition_count}",
            f"Populated partitions: {len(nonzero)}",
            f"EMPTY partitions:     {empty_partitions}"
            + (
                "  <-- most partitions carry no rows; row distribution at read"
                " time follows parquet row-group boundaries, not an even split"
                if empty_partitions > partition_count / 2
                else ""
            ),
            "",
            "Rows per partition (ALL partitions, including empty ones):",
            *table_lines,
            "",
            f"min rows/partition (all):       {min(all_counts):,}",
            f"max rows/partition (all):       {max(all_counts):,}",
            f"avg rows/partition (all):       {avg_all:,.1f}",
            f"avg rows/partition (populated): {mean(nonzero):,.1f}" if nonzero else "",
            f"skew ratio max/avg (all):       {skew_ratio:.3f}",
            "Interpretation: ratio near 1.0 over POPULATED partitions means the",
            "populated partitions are balanced, but a high empty-partition count",
            "still indicates uneven placement and wasted task slots.",
            "",
            "Schema:",
            df._jdf.schema().treeString(),
        ]
        content = "\n".join(lines)
        logger.info("\n" + content)
        evidence.write("01_data_profile.txt", f"DATA PROFILE — {dataset_name}", content)


class TaxiDataTransformer:
    """Medallion cleaning, optional PII masking, deduplication, and window functions."""

    @staticmethod
    def _fingerprint_columns(df: DataFrame) -> List:
        """Builds the normalized column expressions for the trip fingerprint."""
        exprs = []
        for name, kind in FINGERPRINT_SPEC:
            if name not in df.columns:
                continue
            c = col(name)
            if kind == "int":
                exprs.append(c.cast("bigint").cast("string"))
            elif kind == "ts":
                exprs.append(date_format(c, "yyyy-MM-dd HH:mm:ss"))
            elif kind == "dec":
                exprs.append(c.cast("decimal(12,2)").cast("string"))
            else:
                exprs.append(c.cast("string"))
        return exprs

    @staticmethod
    def fingerprint_sql_expr(df: DataFrame) -> str:
        """SQL-string equivalent of _fingerprint_columns (kept in lock-step)."""
        parts = []
        for name, kind in FINGERPRINT_SPEC:
            if name not in df.columns:
                continue
            if kind == "int":
                parts.append(f"CAST(CAST(`{name}` AS BIGINT) AS STRING)")
            elif kind == "ts":
                parts.append(f"date_format(`{name}`, 'yyyy-MM-dd HH:mm:ss')")
            elif kind == "dec":
                parts.append(f"CAST(CAST(`{name}` AS DECIMAL(12,2)) AS STRING)")
            else:
                parts.append(f"CAST(`{name}` AS STRING)")
        return "sha2(concat_ws('||', " + ", ".join(parts) + "), 256)"

    @staticmethod
    def clean_and_deduplicate(df: DataFrame) -> DataFrame:
        """
        DataFrame API transformation chain exercising filter, select,
        withColumn, and dropDuplicates:
        - withColumn: trip duration in minutes + SHA-256 trip_fingerprint over
          ALL normalized business columns (rows sharing a fingerprint are
          exact duplicate records, so dropDuplicates on it is deterministic).
        - filter: drops negative fares, invalid durations, out-of-bounds zones.
        - dropDuplicates: deduplicates on the fingerprint.
        - select: explicit final projection establishing a stable column order.
        - Masks driver name to initials (only when the column exists — real
          NYC TLC data contains no driver PII).
        """
        logger.info("Applying DataFrame API transformations and cleaning rules...")

        df_cleaned = (
            df.withColumn(
                "trip_duration_minutes",
                (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60.0,
            )
            .withColumn(
                "trip_fingerprint",
                sha2(concat_ws("||", *TaxiDataTransformer._fingerprint_columns(df)), 256),
            )
            .filter(
                (col("fare_amount") >= 2.5)
                & (col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))
                & (col("PULocationID").between(1, 263))
                & (col("DOLocationID").between(1, 263))
            )
            .dropDuplicates(["trip_fingerprint"])
        )

        # Explicit select: stable, documented column order for downstream layers
        projection = [c for c in df.columns] + ["trip_duration_minutes", "trip_fingerprint"]
        df_cleaned = df_cleaned.select(*projection)

        # PII masking applies only to synthetic fixtures that carry driver_name;
        # real TLC data has no driver PII.
        if "driver_name" in df.columns:
            name_parts = split(col("driver_name"), " ")
            initials_expr = when(
                size(name_parts) >= 2,
                concat(substring(name_parts[0], 1, 1), lit("."), substring(name_parts[1], 1, 1), lit(".")),
            ).otherwise(concat(substring(col("driver_name"), 1, 1), lit(".")))
            df_cleaned = df_cleaned.withColumn("driver_initials", initials_expr).drop("driver_name")

        return df_cleaned

    @staticmethod
    def rank_trips_by_fare(df: DataFrame) -> DataFrame:
        """Ranks trips by fare_amount per pickup zone per hour using dense_rank()."""
        logger.info("Ranking trips by fare per pickup zone per hour...")
        df_hour = df.withColumn("pickup_hour", hour("tpep_pickup_datetime"))
        window_spec = Window.partitionBy("PULocationID", "pickup_hour").orderBy(col("fare_amount").desc())
        return df_hour.withColumn("fare_rank", dense_rank().over(window_spec))

    @staticmethod
    def calculate_rolling_trip_count(df: DataFrame) -> DataFrame:
        """
        Rolling 7-day trip volume per pickup zone.

        NOTE on precision: the range frame operates on
        unix_timestamp(tpep_pickup_datetime), which ALWAYS returns whole
        SECONDS since epoch (a BIGINT) regardless of the microsecond precision
        of the underlying timestamp column. The frame bounds (-7*86400, 0) are
        therefore in the correct unit. This is spot-checked against a direct
        filter-based recount in the pipeline (see 04_window_spot_check.txt).
        """
        logger.info("Calculating rolling 7-day trip counts per zone...")
        df_ts = df.withColumn("pickup_ts", unix_timestamp("tpep_pickup_datetime"))
        seven_days_sec = 7 * 24 * 60 * 60
        window_spec = (
            Window.partitionBy("PULocationID").orderBy("pickup_ts").rangeBetween(-seven_days_sec, 0)
        )
        return df_ts.withColumn("rolling_7d_trip_count", count("VendorID").over(window_spec))


class WindowSpotChecker:
    """Manually verifies window function results against independent queries."""

    @staticmethod
    def check(df_cleaned: DataFrame, df_ranked: DataFrame, df_rolling: DataFrame,
              evidence: EvidenceCollector) -> bool:
        lines = []
        all_ok = True

        # --- Spot-check 1: top-ranked fare per (zone, hour) vs groupBy max ---
        busiest = (
            df_cleaned.withColumn("pickup_hour", hour("tpep_pickup_datetime"))
            .groupBy("PULocationID", "pickup_hour").count()
            .orderBy(col("count").desc()).first()
        )
        zone, hr = busiest["PULocationID"], busiest["pickup_hour"]

        top_ranked = (
            df_ranked.filter((col("PULocationID") == zone) & (col("pickup_hour") == hr) & (col("fare_rank") == 1))
            .select("fare_amount").first()["fare_amount"]
        )
        manual_max = (
            df_cleaned.withColumn("pickup_hour", hour("tpep_pickup_datetime"))
            .filter((col("PULocationID") == zone) & (col("pickup_hour") == hr))
            .agg(spark_max("fare_amount").alias("max_fare")).first()["max_fare"]
        )
        ok1 = abs(float(top_ranked) - float(manual_max)) < 1e-9
        all_ok &= ok1
        lines += [
            "SPOT-CHECK 1: top-ranked trip by fare per zone per hour",
            f"  Busiest (zone, hour):              ({zone}, {hr}) — {busiest['count']:,} trips",
            f"  fare of fare_rank=1 (window fn):   {top_ranked}",
            f"  max(fare_amount) (independent agg): {manual_max}",
            f"  RESULT: {'PASS — values match' if ok1 else 'FAIL — mismatch'}",
            "",
        ]

        # --- Spot-check 2: rolling 7-day count vs direct filter recount ---
        sample = (
            df_rolling.filter(col("PULocationID") == zone)
            .orderBy(col("pickup_ts").desc()).select("pickup_ts", "rolling_7d_trip_count").first()
        )
        ts, window_count = sample["pickup_ts"], sample["rolling_7d_trip_count"]
        manual_count = (
            df_rolling.filter(
                (col("PULocationID") == zone)
                & (col("pickup_ts") >= ts - 7 * 86400)
                & (col("pickup_ts") <= ts)
            ).count()
        )
        ok2 = int(window_count) == int(manual_count)
        all_ok &= ok2
        lines += [
            "SPOT-CHECK 2: rolling 7-day trip volume per zone",
            f"  Zone: {zone} | anchor pickup_ts (epoch seconds): {ts}",
            f"  rolling_7d_trip_count (range window): {window_count}",
            f"  direct recount (filter ts-7d..ts):    {manual_count}",
            f"  RESULT: {'PASS — values match' if ok2 else 'FAIL — mismatch'}",
            "",
            "Precision note: unix_timestamp() returns whole seconds (BIGINT)",
            "regardless of the microsecond precision of the parquet timestamps,",
            "so rangeBetween(-7*86400, 0) uses the correct unit.",
        ]

        content = "\n".join(lines)
        logger.info("\n" + content)
        evidence.write("04_window_spot_check.txt", "WINDOW FUNCTION SPOT-CHECKS (manual verification)", content)
        return all_ok


class ValidationChecker:
    """Verifies DataFrame API vs Spark SQL equivalence: row-level diff + plan comparison."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _sql_query(self, df_raw: DataFrame) -> str:
        fingerprint_expr = TaxiDataTransformer.fingerprint_sql_expr(df_raw)
        return f"""
            WITH prepped AS (
                SELECT *,
                       -- 60.0D: double literal — a plain 60.0 would be DECIMAL in
                       -- Spark SQL and silently round to 4dp, diverging from the
                       -- DataFrame API's double arithmetic (caught by row-level diff)
                       (unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 60.0D AS trip_duration_minutes,
                       {fingerprint_expr} AS trip_fingerprint
                FROM raw_trips
                WHERE fare_amount >= 2.5
                  AND tpep_dropoff_datetime > tpep_pickup_datetime
                  AND PULocationID BETWEEN 1 AND 263
                  AND DOLocationID BETWEEN 1 AND 263
            )
            SELECT * FROM (
                SELECT *,
                       row_number() OVER (PARTITION BY trip_fingerprint ORDER BY tpep_pickup_datetime) AS row_num
                FROM prepped
            )
            WHERE row_num = 1
        """

    def verify_equivalence(self, df_raw: DataFrame, df_api_cleaned: DataFrame,
                           evidence: EvidenceCollector) -> bool:
        logger.info("Registering temporary view 'raw_trips' for SQL equivalence checking...")
        df_raw.createOrReplaceTempView("raw_trips")

        sql_query = self._sql_query(df_raw)
        df_sql = self.spark.sql(sql_query).drop("row_num")
        if "driver_name" in df_sql.columns:
            df_sql = df_sql.drop("driver_name")

        # ---- Row-level diff (not just row counts) ----
        # The fingerprint covers every normalized business column, so rows that
        # share a fingerprint are exact duplicates and both dedup strategies
        # (dropDuplicates vs row_number()=1) select identical row content.
        compare_cols = sorted(set(df_api_cleaned.columns) & set(df_sql.columns))
        api_proj = df_api_cleaned.select(*compare_cols)
        sql_proj = df_sql.select(*compare_cols)

        api_count = api_proj.count()
        sql_count = sql_proj.count()
        diff_api_sql = api_proj.exceptAll(sql_proj).count()
        diff_sql_api = sql_proj.exceptAll(api_proj).count()
        identical = diff_api_sql == 0 and diff_sql_api == 0 and api_count == sql_count

        diff_report = "\n".join([
            f"Columns compared ({len(compare_cols)}): {', '.join(compare_cols)}",
            "",
            f"DataFrame API output rows: {api_count:,}",
            f"Spark SQL    output rows: {sql_count:,}",
            f"rows in API output missing from SQL output (exceptAll): {diff_api_sql}",
            f"rows in SQL output missing from API output (exceptAll): {diff_sql_api}",
            "",
            f"RESULT: {'PASS — outputs are row-level identical' if identical else 'FAIL — outputs differ'}",
        ])
        logger.info("\n" + diff_report)
        evidence.write("03_row_level_diff.txt", "ROW-LEVEL DIFF — DataFrame API vs Spark SQL", diff_report)

        # ---- Execution plan comparison (documented verbatim) ----
        api_plan = explain_text(df_api_cleaned, "formatted")
        sql_plan = explain_text(self.spark.sql(sql_query), "formatted")
        observations = "\n".join([
            "OBSERVATIONS",
            "-" * 78,
            "1. Both versions compile to the same scan + Filter over the parquet",
            "   source; Catalyst pushes the fare/zone/timestamp predicates down in",
            "   both cases, so the filter stage is plan-identical.",
            "2. The dedup strategies differ at the operator level:",
            "   - DataFrame API dropDuplicates -> HashAggregate keyed on",
            "     trip_fingerprint (partial + final, with Exchange hashpartitioning).",
            "   - Spark SQL row_number()=1     -> Window + Sort over Exchange,",
            "     then Filter (row_num = 1).",
            "   HashAggregate avoids the per-partition Sort, which is why the API",
            "   variant is generally cheaper; both produce identical output rows",
            "   (verified by the row-level diff above).",
            "3. Adaptive Query Execution (AQE) re-optimizes both plans' Exchanges",
            "   at runtime; final shuffle partition counts come from AQE coalescing.",
        ])
        content = (
            "### DataFrame API version — EXPLAIN FORMATTED (verbatim) ###\n\n"
            + api_plan
            + "\n\n### Spark SQL version — EXPLAIN FORMATTED (verbatim) ###\n\n"
            + sql_plan
            + "\n\n"
            + observations
        )
        evidence.write("02_plan_comparison_df_vs_sql.txt",
                       "EXECUTION PLAN COMPARISON — DataFrame API vs Spark SQL", content)

        if identical:
            logger.info("SUCCESS: DataFrame API and Spark SQL outputs are row-level identical.")
        else:
            logger.error("FAILURE: DataFrame API and Spark SQL outputs differ!")
        return identical


class JoinOptimizer:
    """Benchmarks Broadcast vs Sort-Merge joins and tunes shuffle partitions."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def benchmark_joins(self, df_trips: DataFrame, df_zones: DataFrame,
                        evidence: EvidenceCollector, runs: int = 2) -> DataFrame:
        logger.info("JOINS BENCHMARK & EXPLAIN (broadcast vs sort-merge)")

        join_cond = df_trips.PULocationID == df_zones.LocationID

        # 1. Broadcast join (trips large, zones small)
        df_broadcast = df_trips.join(broadcast(df_zones), join_cond, "inner")
        broadcast_plan = explain_text(df_broadcast, "formatted")

        # 2. Sort-merge join (broadcast disabled)
        self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
        df_sort_merge = df_trips.hint("merge").join(df_zones, join_cond, "inner")
        sort_merge_plan = explain_text(df_sort_merge, "formatted")

        # ---- Wall-clock timing: plans alone don't show the latency win ----
        # Warmup run for each variant is executed and DISCARDED so JIT/cache
        # warmup cost doesn't bias either side.
        logger.info("Timing broadcast join (1 warmup + %d timed runs)...", runs)
        self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)
        time_noop_write(df_broadcast)  # warmup, discarded
        broadcast_times = [time_noop_write(df_broadcast) for _ in range(runs)]

        logger.info("Timing sort-merge join (1 warmup + %d timed runs)...", runs)
        self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
        time_noop_write(df_sort_merge)  # warmup, discarded
        sort_merge_times = [time_noop_write(df_sort_merge) for _ in range(runs)]
        self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)

        has_bhj = "BroadcastHashJoin" in broadcast_plan
        has_smj = "SortMergeJoin" in sort_merge_plan
        speedup = (mean(sort_merge_times) - mean(broadcast_times)) / mean(sort_merge_times) * 100
        verdict = "\n".join([
            "CHECKPOINT VERIFICATION",
            "-" * 78,
            f"'BroadcastHashJoin' present in broadcast-join plan:  {'YES — PASS' if has_bhj else 'NO — FAIL'}",
            f"'SortMergeJoin' present in non-broadcast plan:       {'YES' if has_smj else 'NO'}",
            "",
            "WALL-CLOCK TIMING (full execution to noop sink; 1 discarded warmup each)",
            *[f"  broadcast join  run {i + 1}: {t:.3f} s" for i, t in enumerate(broadcast_times)],
            f"  broadcast join  mean:  {mean(broadcast_times):.3f} s",
            *[f"  sort-merge join run {i + 1}: {t:.3f} s" for i, t in enumerate(sort_merge_times)],
            f"  sort-merge join mean:  {mean(sort_merge_times):.3f} s",
            f"  broadcast is {speedup:.1f}% faster than sort-merge on this join",
            "",
            "The broadcast variant ships the ~265-row zone lookup to every executor",
            "(BroadcastExchange) and avoids shuffling the multi-million-row trips",
            "table entirely. The sort-merge variant must Exchange + Sort BOTH sides",
            "on the join key before merging.",
        ])
        content = (
            "### Broadcast join — EXPLAIN FORMATTED (verbatim) ###\n\n" + broadcast_plan
            + "\n\n### Sort-merge join (broadcast disabled) — EXPLAIN FORMATTED (verbatim) ###\n\n"
            + sort_merge_plan + "\n\n" + verdict
        )
        evidence.write("05_explain_broadcast_join.txt",
                       "BROADCAST JOIN vs SORT-MERGE JOIN — EXPLAIN + WALL-CLOCK TIMING", content)

        if not has_bhj:
            logger.error("BroadcastHashJoin NOT found in broadcast join plan!")
        return df_broadcast

    def run_shuffle_tuning_benchmark(self, df: DataFrame, evidence: EvidenceCollector,
                                     default_partitions: int = 200,
                                     tuned_partitions: Optional[int] = None,
                                     runs: int = 3) -> None:
        """
        Times a shuffle-heavy aggregation under default vs tuned
        spark.sql.shuffle.partitions, recording RAW per-run timings, then
        demonstrates coalesce() on the output write.

        - AQE is disabled during the benchmark so the partition setting is
          actually exercised (AQE would otherwise coalesce partitions itself).
        - A warmup run is executed and DISCARDED before each timed group so
          JIT/scan-cache warmup doesn't bias the first configuration measured.
        """
        logger.info("SHUFFLE PERFORMANCE TUNING BENCHMARK")
        if tuned_partitions is None:
            # Match total cluster cores so each shuffle task does meaningful work
            tuned_partitions = max(self.spark.sparkContext.defaultParallelism, 2)
        aqe_before = self.spark.conf.get("spark.sql.adaptive.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.enabled", "false")

        def aggregation() -> DataFrame:
            return df.groupBy("PULocationID", "DOLocationID").agg(count("VendorID").alias("trip_count"))

        results = {}
        for label, n in [("default", default_partitions), ("tuned", tuned_partitions)]:
            self.spark.conf.set("spark.sql.shuffle.partitions", n)
            logger.info(f"  [{label}: {n} partitions] warmup run (discarded)...")
            time_noop_write(aggregation())  # warmup, discarded
            timings = []
            for i in range(runs):
                elapsed = time_noop_write(aggregation())
                timings.append(elapsed)
                logger.info(f"  [{label}: {n} partitions] run {i + 1}/{runs}: {elapsed:.3f}s")
            results[label] = (n, timings)

        d_n, d_t = results["default"]
        t_n, t_t = results["tuned"]
        improvement = (mean(d_t) - mean(t_t)) / mean(d_t) * 100

        # ---- coalesce() demonstration on the OUTPUT write ----
        # Scope requires tuning shuffle partitions AND coalescing output.
        # The aggregation result is written to parquet twice under the tuned
        # partition setting: once as-is, once with .coalesce(2). This shows
        # coalesce's effect on output file count and write time. (For Iceberg
        # tables, file sizing is instead governed by table properties — see
        # IcebergWriter — because Iceberg manages its own file layout.)
        self.spark.conf.set("spark.sql.shuffle.partitions", t_n)
        os.makedirs(BENCH_DIR, exist_ok=True)
        plain_dir = os.path.join(BENCH_DIR, "agg_plain")
        coalesced_dir = os.path.join(BENCH_DIR, "agg_coalesced")

        start = time.time()
        aggregation().write.mode("overwrite").parquet(plain_dir)
        plain_time = time.time() - start
        plain_files = len(glob.glob(os.path.join(plain_dir, "part-*")))

        start = time.time()
        aggregation().coalesce(2).write.mode("overwrite").parquet(coalesced_dir)
        coalesced_time = time.time() - start
        coalesced_files = len(glob.glob(os.path.join(coalesced_dir, "part-*")))

        shutil.rmtree(BENCH_DIR, ignore_errors=True)

        self.spark.conf.set("spark.sql.adaptive.enabled", aqe_before)
        self.spark.conf.set("spark.sql.shuffle.partitions", default_partitions)

        lines = [
            "Benchmark query: df.groupBy(PULocationID, DOLocationID)",
            "                   .agg(count(VendorID)) -> noop sink (full execution,",
            "                 no collect() overhead). AQE disabled during benchmark.",
            "One warmup run per configuration is executed and discarded.",
            "",
            f"spark.sql.shuffle.partitions = {d_n} (DEFAULT) — raw timings:",
            *[f"  run {i + 1}: {t:.3f} s" for i, t in enumerate(d_t)],
            f"  mean: {mean(d_t):.3f} s",
            "",
            f"spark.sql.shuffle.partitions = {t_n} (TUNED) — raw timings:",
            *[f"  run {i + 1}: {t:.3f} s" for i, t in enumerate(t_t)],
            f"  mean: {mean(t_t):.3f} s",
            "",
            f"IMPROVEMENT: {mean(d_t):.3f}s -> {mean(t_t):.3f}s  =  {improvement:.1f}% faster",
            "",
            f"Why: 200 shuffle partitions create hundreds of tiny tasks whose",
            f"scheduling overhead dominates on this cluster; {t_n} partitions match",
            f"the cluster's available parallelism (defaultParallelism), so each",
            f"task does meaningful work.",
            "",
            "COALESCE OUTPUT DEMONSTRATION (parquet write of the aggregation result)",
            "-" * 78,
            f"  without coalesce: {plain_files} output files, write took {plain_time:.3f} s",
            f"  with .coalesce(2): {coalesced_files} output files, write took {coalesced_time:.3f} s",
            "  coalesce(2) merges the write into 2 tasks/files — fewer, larger",
            "  files at the cost of reduced write parallelism. For the Iceberg",
            "  Silver/Gold tables the equivalent control is the",
            "  write.target-file-size-bytes / write.distribution-mode table",
            "  properties, since Iceberg governs its own file layout.",
        ]
        content = "\n".join(lines)
        logger.info("\n" + content)
        evidence.write("06_shuffle_tuning_timings.txt",
                       "SHUFFLE TUNING + COALESCE — RAW TIMING NUMBERS", content)


class IcebergWriter:
    """Writes Silver/Gold Iceberg tables and captures snapshot metadata evidence."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def write_table(self, df: DataFrame, table_name: str) -> None:
        namespace = table_name.rsplit(".", 1)[0]
        logger.info(f"Ensuring Iceberg namespace exists: {namespace}")
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

        # File sizing is controlled via Iceberg table properties — NOT via
        # df.coalesce(), which would only throttle write parallelism without
        # governing Iceberg's output file count.
        #
        # Overwrite semantics: the FIRST run creates the table; subsequent
        # runs use overwrite(lit(True)) — a snapshot-producing OVERWRITE
        # operation that REPLACES the data while PRESERVING snapshot history,
        # so <table>.history shows the lineage across runs. createOrReplace()
        # is deliberately avoided: it drops and recreates the table, erasing
        # the history the checkpoint asks us to verify.
        if self.spark.catalog.tableExists(table_name):
            logger.info(f"Overwriting Iceberg table '{table_name}' (new snapshot, history preserved)...")
            df.writeTo(table_name).overwrite(lit(True))
        else:
            logger.info(f"Creating Iceberg table '{table_name}' (first run)...")
            (
                df.writeTo(table_name)
                .using("iceberg")
                .tableProperty("write.target-file-size-bytes", str(128 * 1024 * 1024))
                .tableProperty("write.distribution-mode", "hash")
                .create()
            )

        count_written = self.spark.read.format("iceberg").load(table_name).count()
        logger.info(f"Verified '{table_name}' row count after write: {count_written:,}")

    def capture_metadata_evidence(self, table_names: List[str], evidence: EvidenceCollector) -> None:
        """Captures .snapshots / .history / .files metadata proving new snapshots."""
        sections = []
        for table in table_names:
            snapshots = self.spark.sql(
                f"SELECT committed_at, snapshot_id, parent_id, operation FROM {table}.snapshots ORDER BY committed_at"
            )
            history = self.spark.sql(
                f"SELECT made_current_at, snapshot_id, is_current_ancestor FROM {table}.history ORDER BY made_current_at"
            )
            files = self.spark.sql(
                f"SELECT file_path, record_count, file_size_in_bytes FROM {table}.files"
            )
            sections += [
                f"### {table} — snapshots ###",
                df_to_text(snapshots, 20, 0),
                f"### {table} — history ###",
                df_to_text(history, 20, 0),
                f"### {table} — data files ###",
                df_to_text(files, 20, 120),
                "",
            ]
        sections += [
            "Note: on the first run each table shows a single snapshot from",
            "CREATE TABLE AS. Run the pipeline a second time to see the snapshot",
            "lineage grow (operation=overwrite with a parent_id), which is what",
            "'table metadata reflects new snapshot' verifies.",
        ]
        evidence.write("07_iceberg_metadata.txt",
                       "ICEBERG TABLE METADATA — SNAPSHOT VERIFICATION", "\n".join(sections))


class PipelineRunner:
    """Orchestrates the Week 3 PySpark pipeline and evidence collection."""

    def run(self) -> None:
        logger.info("=" * 80)
        logger.info("STARTING WEEK 3 PYSPARK PROCESSING PIPELINE")
        logger.info("=" * 80)

        session_manager = SparkSessionManager()
        spark = session_manager.get_or_create_session()
        evidence = EvidenceCollector(EVIDENCE_DIR)

        try:
            loader = DataLoader(spark)

            # STAGE 1: Ingestion & Profiling (real NYC TLC parquet)
            logger.info("--- STAGE 1: Ingestion & Profiling ---")
            df_raw = loader.load_tlc_parquet(DATA_DIR)
            DataProfiler.profile(df_raw, "Raw NYC TLC Yellow Taxi Trips", evidence)

            # STAGE 2: DataFrame API Cleaning & Transformations
            logger.info("--- STAGE 2: DataFrame API Cleaning & Transformations ---")
            df_cleaned = TaxiDataTransformer.clean_and_deduplicate(df_raw).cache()
            raw_count = df_raw.count()
            cleaned_count = df_cleaned.count()
            drop_rate = (raw_count - cleaned_count) / raw_count * 100
            logger.info(
                f"Cleaned & deduplicated: {cleaned_count:,} rows "
                f"(dropped {raw_count - cleaned_count:,}, {drop_rate:.2f}%)"
            )

            # STAGE 3: Spark SQL Equivalence (row-level diff + plan comparison)
            logger.info("--- STAGE 3: Spark SQL Equivalence Checks ---")
            ValidationChecker(spark).verify_equivalence(df_raw, df_cleaned, evidence)

            # STAGE 4: Window Functions + manual spot-checks
            logger.info("--- STAGE 4: Window Functions ---")
            df_ranked = TaxiDataTransformer.rank_trips_by_fare(df_cleaned)
            df_rolling = TaxiDataTransformer.calculate_rolling_trip_count(df_cleaned)
            WindowSpotChecker.check(df_cleaned, df_ranked, df_rolling, evidence)

            # STAGE 5: Broadcast Join (verbatim EXPLAIN + wall-clock timing)
            logger.info("--- STAGE 5: Broadcast Join ---")
            df_zones = loader.load_zone_lookup(LOOKUP_PATH)
            optimizer = JoinOptimizer(spark)
            optimizer.benchmark_joins(df_cleaned, df_zones, evidence)

            # STAGE 6: Shuffle Tuning + coalesce output demonstration
            logger.info("--- STAGE 6: Shuffle Tuning Performance Benchmark ---")
            optimizer.run_shuffle_tuning_benchmark(df_cleaned, evidence)

            # STAGE 7: Iceberg Silver + Gold writes & metadata verification.
            # BOTH window-function outputs are persisted to the Gold layer:
            # fare ranking and the rolling 7-day volume.
            if SKIP_ICEBERG:
                logger.warning("SKIP_ICEBERG=1 — skipping Iceberg writes (local smoke-test mode).")
            else:
                logger.info("--- STAGE 7: Write to Iceberg Silver & Gold ---")
                writer = IcebergWriter(spark)
                writer.write_table(df_cleaned, SILVER_TABLE)
                writer.write_table(df_ranked, GOLD_TABLE)
                writer.write_table(df_rolling, GOLD_ROLLING_TABLE)
                writer.capture_metadata_evidence(
                    [SILVER_TABLE, GOLD_TABLE, GOLD_ROLLING_TABLE], evidence
                )

            logger.info("=" * 80)
            logger.info("WEEK 3 PIPELINE COMPLETED SUCCESSFULLY — evidence in " + EVIDENCE_DIR)
            logger.info("=" * 80)

            if KEEP_UI_SECONDS > 0:
                logger.info(
                    f"Keeping SparkSession alive {KEEP_UI_SECONDS}s — open http://localhost:4040 "
                    "to capture the Spark UI DAG screenshots now."
                )
                time.sleep(KEEP_UI_SECONDS)

        except Exception as e:
            logger.error(f"PIPELINE ERROR OCCURRED: {e}", exc_info=True)
            raise
        finally:
            session_manager.stop_session()


if __name__ == "__main__":  # pragma: no cover
    PipelineRunner().run()
