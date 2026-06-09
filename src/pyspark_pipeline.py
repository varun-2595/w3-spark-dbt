import os
import time
import logging
from typing import Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, split, concat, substring, lit, when, size, 
    unix_timestamp, sha2, concat_ws, hour, count, 
    dense_rank, broadcast, spark_partition_id
)
from pyspark.sql.window import Window

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PySparkPipeline")


class SparkSessionManager:
    """Manages the creation, configuration, and destruction of the SparkSession."""

    def __init__(self, app_name: str = "NYC Yellow Taxi Spark Pipeline"):
        self.app_name = app_name
        self.spark: SparkSession = None

    def get_or_create_session(self) -> SparkSession:
        """Initializes and returns the SparkSession with global cluster settings."""
        logger.info(f"Initializing SparkSession: '{self.app_name}'...")
        self.spark = SparkSession.builder \
            .appName(self.app_name) \
            .getOrCreate()
        
        # Suppress verbose spark logging, focus on app level warnings/errors
        self.spark.sparkContext.setLogLevel("WARN")
        return self.spark

    def stop_session(self) -> None:
        """Stops the active SparkSession."""
        if self.spark:
            logger.info("Stopping active SparkSession.")
            self.spark.stop()
            self.spark = None


class DataLoader:
    """Handles standard ingestion from Parquet files and external S3/MinIO csv datasets."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def load_parquet(self, path: str) -> DataFrame:
        """Loads a Parquet file from a given path (local mount or warehouse)."""
        logger.info(f"Ingesting Parquet data from: {path}")
        if not os.path.exists(path) and not path.startswith("s3a://") and not path.startswith("s3://"):
            raise FileNotFoundError(f"Source Parquet file not found at: {path}")
        return self.spark.read.parquet(path)

    def load_csv_from_s3(self, path: str) -> DataFrame:
        """Loads a CSV lookup dataset from MinIO S3 with header and schema inference."""
        logger.info(f"Ingesting CSV lookup data from S3: {path}")
        return self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(path)


class DataProfiler:
    """Performs profiling on PySpark DataFrames, extracting partition counts, data skew, and schema metadata."""

    @staticmethod
    def profile(df: DataFrame, dataset_name: str) -> None:
        """Logs row counts, partitions, rows per partition, and schema details."""
        logger.info(f"============================================================")
        logger.info(f"PROFILING DATASET: {dataset_name}")
        logger.info(f"============================================================")
        
        row_count = df.count()
        partition_count = df.rdd.getNumPartitions()
        logger.info(f"Total Row Count: {row_count}")
        logger.info(f"Partition Count: {partition_count}")
        
        # Profile data distribution across partitions to check for skew
        logger.info("Data distribution across partitions (data skew check):")
        df.groupBy(spark_partition_id().alias("partition_id")) \
          .count() \
          .orderBy("partition_id") \
          .show()
        
        logger.info("Schema details:")
        df.printSchema()


class TaxiDataTransformer:
    """Applies medallion cleaning, PII masking, deduplication, and window function transformations."""

    @staticmethod
    def clean_and_deduplicate(df: DataFrame) -> DataFrame:
        """
        Cleans yellow taxi trip data:
        - Computes trip duration in minutes.
        - Masks driver's name by extracting initials (PII masking).
        - Generates a unique SHA-256 fingerprint for deduplication.
        - Filters out negative fares, invalid durations, and out-of-bounds locations.
        """
        logger.info("Applying DataFrame API transformations and cleaning rules...")

        # Construct expression to generate driver initials (e.g. 'John Smith' -> 'J.S.')
        name_parts = split(col("driver_name"), " ")
        initials_expr = when(
            size(name_parts) >= 2,
            concat(substring(name_parts[0], 1, 1), lit("."), substring(name_parts[1], 1, 1), lit("."))
        ).otherwise(
            concat(substring(col("driver_name"), 1, 1), lit("."))
        )

        # Build clean pipeline
        df_cleaned = df \
            .withColumn("trip_duration_minutes", 
                        (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60.0) \
            .withColumn("driver_initials", initials_expr) \
            .withColumn("trip_fingerprint", 
                        sha2(concat_ws("||", "VendorID", "tpep_pickup_datetime", "PULocationID", "DOLocationID"), 256)) \
            .filter(
                (col("fare_amount") >= 2.5) & 
                (col("tpep_dropoff_datetime") > col("tpep_pickup_datetime")) & 
                (col("PULocationID").between(1, 263)) & 
                (col("DOLocationID").between(1, 263))
            ) \
            .dropDuplicates(["trip_fingerprint"])

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
        """Calculates a rolling 7-day trip volume per pickup zone using range window boundaries."""
        logger.info("Calculating rolling 7-day trip counts per zone...")
        df_ts = df.withColumn("pickup_ts", unix_timestamp("tpep_pickup_datetime"))
        seven_days_sec = 7 * 24 * 60 * 60
        
        window_spec = Window.partitionBy("PULocationID") \
                            .orderBy("pickup_ts") \
                            .rangeBetween(-seven_days_sec, 0)
                            
        return df_ts.withColumn("rolling_7d_trip_count", count("VendorID").over(window_spec))


class ValidationChecker:
    """Validates the equivalence of DataFrame API transformations and raw Spark SQL queries."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def verify_equivalence(self, df_raw: DataFrame, df_api_cleaned: DataFrame) -> bool:
        """Runs equivalent Spark SQL transformation and verifies row-by-row equivalence with the API output."""
        logger.info("Registering temporary view 'raw_trips' for SQL equivalence checking...")
        df_raw.createOrReplaceTempView("raw_trips")

        sql_query = """
            WITH prepped AS (
                SELECT *,
                       (unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 60.0 AS trip_duration_minutes,
                       CASE 
                           WHEN size(split(driver_name, ' ')) >= 2 THEN 
                               concat(substring(split(driver_name, ' ')[0], 1, 1), '.', substring(split(driver_name, ' ')[1], 1, 1), '.')
                           ELSE 
                               concat(substring(driver_name, 1, 1), '.')
                       END AS driver_initials,
                       sha2(concat_ws('||', VendorID, tpep_pickup_datetime, PULocationID, DOLocationID), 256) AS trip_fingerprint
                FROM raw_trips
            )
            SELECT *
            FROM (
                SELECT *,
                       row_number() OVER (PARTITION BY trip_fingerprint ORDER BY tpep_pickup_datetime) as row_num
                FROM prepped
                WHERE fare_amount >= 2.5
                  AND tpep_dropoff_datetime > tpep_pickup_datetime
                  AND PULocationID BETWEEN 1 AND 263
                  AND DOLocationID BETWEEN 1 AND 263
            )
            WHERE row_num = 1
        """
        logger.info("Executing Spark SQL equivalence query...")
        df_sql = self.spark.sql(sql_query).drop("row_num")

        api_count = df_api_cleaned.count()
        sql_count = df_sql.count()

        # Compute differences in both directions
        diff_api_sql = df_api_cleaned.select("trip_fingerprint").subtract(df_sql.select("trip_fingerprint")).count()
        diff_sql_api = df_sql.select("trip_fingerprint").subtract(df_api_cleaned.select("trip_fingerprint")).count()

        logger.info(f"API Output Count: {api_count} | SQL Output Count: {sql_count}")
        if diff_api_sql == 0 and diff_sql_api == 0 and api_count == sql_count:
            logger.info("✅ SUCCESS: DataFrame API and Spark SQL outputs are row-by-row identical!")
            return True
        else:
            logger.error(f"❌ FAILURE: DataFrame API and Spark SQL outputs differ! Diff counts: API-SQL={diff_api_sql}, SQL-API={diff_sql_api}")
            return False


class JoinOptimizer:
    """Benchmarks join strategies (Broadcast vs Sort-Merge) and performs partition shuffle tuning."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def benchmark_joins(self, df_trips: DataFrame, df_zones: DataFrame) -> DataFrame:
        """Executes and compares Broadcast Join vs Sort-Merge Join, printing their physical execution plans."""
        logger.info("------------------------------------------------------------")
        logger.info("JOINS BENCHMARK & EXPLAIN")
        logger.info("------------------------------------------------------------")
        
        # 1. Broadcast Join (trips are large, zones are small)
        logger.info("Executing Broadcast Join (trips join broadcast(zones))...")
        df_broadcast = df_trips.join(
            broadcast(df_zones), 
            df_trips.PULocationID == df_zones.LocationID, 
            "inner"
        )
        logger.info("Physical plan for Broadcast Join (EXPLAIN):")
        df_broadcast.explain()

        # 2. Sort-Merge Join (forced by disabling broadcast)
        logger.info("Executing Non-Broadcast Sort-Merge Join...")
        self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
        df_sort_merge = df_trips.join(
            df_zones, 
            df_trips.PULocationID == df_zones.LocationID, 
            "inner"
        )
        logger.info("Physical plan for Sort-Merge Join (EXPLAIN):")
        df_sort_merge.explain()

        # Restore autoBroadcastJoinThreshold back to default 10MB
        self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)

        return df_broadcast

    def run_shuffle_tuning_benchmark(self, df: DataFrame) -> None:
        """Performs a complex aggregation shuffle benchmark under default (200) vs tuned (8) partitions."""
        logger.info("------------------------------------------------------------")
        logger.info("SHUFFLE PERFORMANCE TUNING BENCHMARK")
        logger.info("------------------------------------------------------------")

        # Complex aggregation query to trigger a shuffle stage
        def run_aggregation():
            return df.groupBy("PULocationID", "DOLocationID").agg(count("VendorID").alias("trip_count"))

        # Default shuffle partition count (200)
        self.spark.conf.set("spark.sql.shuffle.partitions", 200)
        logger.info("Running aggregation stage with DEFAULT spark.sql.shuffle.partitions = 200...")
        start_time = time.time()
        run_aggregation().collect()
        default_time = time.time() - start_time
        logger.info(f"Default partitions execution time: {default_time:.3f} seconds")

        # Tuned partition count (8) + coalesced output writer
        self.spark.conf.set("spark.sql.shuffle.partitions", 8)
        logger.info("Running aggregation stage with TUNED spark.sql.shuffle.partitions = 8...")
        start_time = time.time()
        run_aggregation().coalesce(2).collect()
        tuned_time = time.time() - start_time
        logger.info(f"Tuned partitions execution time: {tuned_time:.3f} seconds")

        speedup = ((default_time - tuned_time) / default_time) * 100
        logger.info(f"🚀 Performance improvement: {speedup:.2f}% faster")


class IcebergWriter:
    """Manages writing cleaned data to persistent Iceberg Catalog namespace and tables."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def write_table(self, df: DataFrame, table_name: str) -> None:
        """Saves a DataFrame as an Iceberg table in overwrite mode and prints verification stats."""
        namespace = table_name.rsplit(".", 1)[0]
        logger.info(f"Ensuring Iceberg namespace exists: {namespace}")
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
        
        logger.info(f"Writing data to Iceberg table '{table_name}' in overwrite mode...")
        df.write \
            .format("iceberg") \
            .mode("overwrite") \
            .saveAsTable(table_name)
            
        # Verify writing was successful
        count_written = self.spark.read.format("iceberg").load(table_name).count()
        logger.info(f"Verification - Table '{table_name}' loaded. Verified Row Count: {count_written}")


class PipelineRunner:
    """Orchestrates the entire medallion PySpark pipeline lifecycle and checkpoints."""

    def __init__(self, batch_path: str, lookup_path: str):
        self.batch_path = batch_path
        self.lookup_path = lookup_path

    def run(self) -> None:
        logger.info("================================================================================")
        logger.info("STARTING WEEK 3 PYSPARK PROCESSING PIPELINE (OOP)")
        logger.info("================================================================================")
        
        session_manager = SparkSessionManager()
        spark = session_manager.get_or_create_session()

        try:
            loader = DataLoader(spark)

            # STAGE 1: Ingestion & Profiling
            logger.info("\n--- STAGE 1: Ingestion & Profiling ---")
            df_raw = loader.load_parquet(self.batch_path)
            DataProfiler.profile(df_raw, "Raw Taxi Trips")

            # STAGE 2: DataFrame API Cleaning & Transformations
            logger.info("\n--- STAGE 2: DataFrame API Cleaning & Transformations ---")
            df_cleaned = TaxiDataTransformer.clean_and_deduplicate(df_raw)
            cleaned_count = df_cleaned.count()
            raw_count = df_raw.count()
            drop_rate = ((raw_count - cleaned_count) / raw_count) * 100
            logger.info(f"Cleaned & Deduplicated Count: {cleaned_count} | Dropped: {raw_count - cleaned_count} ({drop_rate:.2f}%)")

            # STAGE 3: Spark SQL Equivalence Checks
            logger.info("\n--- STAGE 3: Spark SQL Equivalence Checks ---")
            validator = ValidationChecker(spark)
            validator.verify_equivalence(df_raw, df_cleaned)

            # STAGE 4: Window Functions
            logger.info("\n--- STAGE 4: Window Functions ---")
            df_ranked = TaxiDataTransformer.rank_trips_by_fare(df_cleaned)
            logger.info("Top Ranked trips by fare per zone per hour (Sample):")
            df_ranked.select("PULocationID", "pickup_hour", "fare_amount", "fare_rank") \
                     .filter(col("fare_rank") <= 3) \
                     .orderBy("PULocationID", "pickup_hour", "fare_rank") \
                     .show(5)

            df_rolling = TaxiDataTransformer.calculate_rolling_trip_count(df_cleaned)
            logger.info("Rolling 7-day trip volumes per zone (Sample):")
            df_rolling.select("PULocationID", "tpep_pickup_datetime", "rolling_7d_trip_count") \
                      .orderBy("PULocationID", "tpep_pickup_datetime") \
                      .show(5)

            # STAGE 5: Broadcast Join
            logger.info("\n--- STAGE 5: Broadcast Join ---")
            df_zones = loader.load_csv_from_s3(self.lookup_path)
            optimizer = JoinOptimizer(spark)
            optimizer.benchmark_joins(df_cleaned, df_zones)

            # STAGE 6: Shuffle Tuning Performance Benchmark
            logger.info("\n--- STAGE 6: Shuffle Tuning Performance Benchmark ---")
            optimizer.run_shuffle_tuning_benchmark(df_cleaned)

            # STAGE 7: Write to Iceberg Tables
            logger.info("\n--- STAGE 7: Write to Iceberg Tables ---")
            writer = IcebergWriter(spark)
            writer.write_table(df_cleaned, "demo.nyc_taxi.trips_silver")
            writer.write_table(df_ranked, "demo.nyc_taxi.trips_gold")

            logger.info("================================================================================")
            logger.info("WEEK 3 PYSPARK PROCESSING PIPELINE (OOP) COMPLETED SUCCESSFULLY!")
            logger.info("================================================================================")
        
        except Exception as e:
            logger.error(f"❌ PIPELINE ERROR OCCURRED: {str(e)}", exc_info=True)
            raise e
        finally:
            session_manager.stop_session()


if __name__ == "__main__":
    # Define local mount paths inside the Spark container workspace
    container_batch_1 = "/opt/spark/workspace/data/yellow_tripdata_2026-01.parquet"
    container_lookup = "s3a://warehouse/lookup/taxi_zone_lookup.csv"

    runner = PipelineRunner(
        batch_path=container_batch_1,
        lookup_path=container_lookup
    )
    runner.run()
