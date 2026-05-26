import os
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, split, concat, substring, lit, when, size, 
    unix_timestamp, sha2, concat_ws, hour, date_format,
    count, dense_rank, broadcast, to_date
)
from pyspark.sql.window import Window

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PySparkPipeline")

def main():
    logger.info("================================================================================")
    logger.info("STARTING WEEK 3 PYSPARK PROCESSING PIPELINE")
    logger.info("================================================================================")
    
    # 1. Initialize SparkSession (REST Catalog configurations loaded from spark-defaults.conf)
    spark = SparkSession.builder \
        .appName("NYC Yellow Taxi Spark Pipeline") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    
    # Define local mount paths inside the container
    batch_1_path = "/opt/spark/workspace/data/yellow_tripdata_2026-01.parquet"
    batch_2_path = "/opt/spark/workspace/data/yellow_tripdata_2026-02.parquet"
    lookup_s3_path = "s3a://warehouse/lookup/taxi_zone_lookup.csv"
    
    logger.info("------------------------------------------------------------")
    logger.info("STAGE 1: Ingestion & Profiling")
    logger.info("------------------------------------------------------------")
    
    # Read batch 1
    logger.info(f"Ingesting Yellow Taxi batch from {batch_1_path}...")
    df_raw = spark.read.parquet(batch_1_path)
    
    # Profile schemas & partitions
    row_count = df_raw.count()
    partition_count = df_raw.rdd.getNumPartitions()
    logger.info(f"Raw Row Count: {row_count}")
    logger.info(f"Raw Partition Count: {partition_count}")
    
    # Check data skew (distribution across partitions)
    logger.info("Profiling rows per partition:")
    df_raw.groupBy(spark_partition_id()).count().show()
    
    # Print schema
    logger.info("Raw schema:")
    df_raw.printSchema()
    
    logger.info("------------------------------------------------------------")
    logger.info("STAGE 2: DataFrame API Cleaning & Transformations")
    logger.info("------------------------------------------------------------")
    
    # Calculate duration, mask PII, generate fingerprint, filter invalid records
    name_parts = split(col("driver_name"), " ")
    initials_expr = when(size(name_parts) >= 2,
                         concat(substring(name_parts[0], 1, 1), lit("."), substring(name_parts[1], 1, 1), lit("."))
                        ).otherwise(concat(substring(col("driver_name"), 1, 1), lit(".")))
                        
    df_cleaned = df_raw \
        .withColumn("trip_duration_minutes", 
                    (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60.0) \
        .withColumn("driver_initials", initials_expr) \
        .withColumn("trip_fingerprint", 
                    sha2(concat_ws("||", "VendorID", "tpep_pickup_datetime", "PULocationID", "DOLocationID"), 256)) \
        .filter(
            (col("fare_amount") >= 2.5) &  # Minimum valid fare
            (col("tpep_dropoff_datetime") > col("tpep_pickup_datetime")) &  # Valid duration
            (col("PULocationID").between(1, 263)) &  # Valid zones
            (col("DOLocationID").between(1, 263))
        ) \
        .dropDuplicates(["trip_fingerprint"])
        
    cleaned_count = df_cleaned.count()
    logger.info(f"Cleaned and Deduplicated Row Count: {cleaned_count}")
    logger.info(f"Dropped {row_count - cleaned_count} invalid/duplicate records (Deduplication/Clean Rate: {((row_count - cleaned_count)/row_count)*100:.2f}%)")
    
    logger.info("------------------------------------------------------------")
    logger.info("STAGE 3: Spark SQL Equivalence Checks")
    logger.info("------------------------------------------------------------")
    
    # Register temporary views
    df_raw.createOrReplaceTempView("raw_trips")
    
    # SQL cleaning query
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
    
    df_sql = spark.sql(sql_query).drop("row_num")
    sql_count = df_sql.count()
    logger.info(f"SQL Equivalent Row Count: {sql_count}")
    
    # Parity check
    diff_api_sql = df_cleaned.select("trip_fingerprint").subtract(df_sql.select("trip_fingerprint")).count()
    diff_sql_api = df_sql.select("trip_fingerprint").subtract(df_cleaned.select("trip_fingerprint")).count()
    
    if diff_api_sql == 0 and diff_sql_api == 0 and cleaned_count == sql_count:
        logger.info("✅ SUCCESS: DataFrame API and Spark SQL outputs are row-by-row identical!")
    else:
        logger.error(f"❌ FAILURE: DataFrame API and Spark SQL outputs differ! Diff counts: API-SQL={diff_api_sql}, SQL-API={diff_sql_api}")
        
    logger.info("------------------------------------------------------------")
    logger.info("STAGE 4: Window Functions")
    logger.info("------------------------------------------------------------")
    
    # 4.1 Rank trips by fare_amount per pickup zone per hour using dense_rank()
    df_hour = df_cleaned.withColumn("pickup_hour", hour("tpep_pickup_datetime"))
    window_rank = Window.partitionBy("PULocationID", "pickup_hour").orderBy(col("fare_amount").desc())
    df_ranked = df_hour.withColumn("fare_rank", dense_rank().over(window_rank))
    
    logger.info("Sample of top-ranked trips by fare per zone per hour:")
    df_ranked.select("PULocationID", "pickup_hour", "fare_amount", "fare_rank") \
             .filter(col("fare_rank") <= 3) \
             .orderBy("PULocationID", "pickup_hour", "fare_rank") \
             .show(10)
             
    # 4.2 Rolling 7-day trip volume per zone
    df_ts = df_cleaned.withColumn("pickup_ts", unix_timestamp("tpep_pickup_datetime"))
    seven_days_sec = 7 * 24 * 60 * 60
    
    window_rolling = Window.partitionBy("PULocationID") \
                           .orderBy("pickup_ts") \
                           .rangeBetween(-seven_days_sec, 0)
                           
    df_rolling = df_ts.withColumn("rolling_7d_trip_count", count("VendorID").over(window_rolling))
    
    logger.info("Sample of rolling 7-day trip counts:")
    df_rolling.select("PULocationID", "tpep_pickup_datetime", "rolling_7d_trip_count") \
              .orderBy("PULocationID", "tpep_pickup_datetime") \
              .show(10)
              
    logger.info("------------------------------------------------------------")
    logger.info("STAGE 5: Broadcast Join")
    logger.info("------------------------------------------------------------")
    
    # Load taxi zone lookup CSV from MinIO S3
    logger.info(f"Loading taxi zone lookup from {lookup_s3_path}...")
    df_zones = spark.read.option("header", "true").option("inferSchema", "true").csv(lookup_s3_path)
    
    # Join using broadcast join
    logger.info("Executing Broadcast Join...")
    df_joined_broadcast = df_cleaned.join(broadcast(df_zones), df_cleaned.PULocationID == df_zones.LocationID, "inner")
    
    # Capture explain plan
    logger.info("Execution Plan for Broadcast Join (EXPLAIN):")
    df_joined_broadcast.explain()
    
    # Execute non-broadcast join (disable broadcast threshold)
    logger.info("Executing Non-Broadcast (Sort-Merge) Join...")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    df_joined_normal = df_cleaned.join(df_zones, df_cleaned.PULocationID == df_zones.LocationID, "inner")
    
    logger.info("Execution Plan for Sort-Merge Join (EXPLAIN):")
    df_joined_normal.explain()
    
    # Re-enable broadcast threshold
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)
    
    logger.info("------------------------------------------------------------")
    logger.info("STAGE 6: Shuffle Tuning Performance Benchmark")
    logger.info("------------------------------------------------------------")
    
    # Complex aggregation query to trigger shuffles
    agg_query = lambda: df_cleaned.groupBy("PULocationID", "DOLocationID").agg(count("VendorID").alias("trip_count"))
    
    # 6.1 Default partitions (200)
    spark.conf.set("spark.sql.shuffle.partitions", 200)
    logger.info("Running aggregation with DEFAULT spark.sql.shuffle.partitions = 200...")
    start_time = time.time()
    agg_query().collect()
    default_time = time.time() - start_time
    logger.info(f"Default partitions execution time: {default_time:.3f} seconds")
    
    # 6.2 Tuned partitions (8) + coalesced output
    spark.conf.set("spark.sql.shuffle.partitions", 8)
    logger.info("Running aggregation with TUNED spark.sql.shuffle.partitions = 8...")
    start_time = time.time()
    agg_query().coalesce(2).collect()
    tuned_time = time.time() - start_time
    logger.info(f"Tuned partitions execution time: {tuned_time:.3f} seconds")
    
    speedup = ((default_time - tuned_time) / default_time) * 100
    logger.info(f"🚀 Performance improvement: {speedup:.2f}% faster")
    
    logger.info("------------------------------------------------------------")
    logger.info("STAGE 7: Write to Iceberg Tables")
    logger.info("------------------------------------------------------------")
    
    # Ensure namespaces exist
    logger.info("Creating Iceberg namespace nyc_taxi if not exists...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.nyc_taxi")
    
    # Write Silver layer (Cleaned Trips)
    silver_table = "demo.nyc_taxi.trips_silver"
    logger.info(f"Writing cleaned data to Silver Iceberg table: {silver_table}...")
    df_cleaned.write \
        .format("iceberg") \
        .mode("overwrite") \
        .saveAsTable(silver_table)
        
    # Write Gold layer (Ranked and aggregated trips)
    gold_table = "demo.nyc_taxi.trips_gold"
    logger.info(f"Writing ranked data to Gold Iceberg table: {gold_table}...")
    df_ranked.write \
        .format("iceberg") \
        .mode("overwrite") \
        .saveAsTable(gold_table)
        
    logger.info("Verifying metadata updates by reading written Iceberg tables:")
    logger.info(f"Silver count: {spark.read.format('iceberg').load(silver_table).count()}")
    logger.info(f"Gold count: {spark.read.format('iceberg').load(gold_table).count()}")
    
    logger.info("================================================================================")
    logger.info("WEEK 3 PYSPARK PROCESSING PIPELINE COMPLETED SUCCESSFULLY!")
    logger.info("================================================================================")

# Helper function to get partition ID
def spark_partition_id():
    from pyspark.sql.functions import spark_partition_id as _spark_partition_id
    return _spark_partition_id()

if __name__ == "__main__":
    main()
