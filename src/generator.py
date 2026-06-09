import logging
import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from src.config import RAW_BATCH_1_PATH, RAW_BATCH_2_PATH

logger = logging.getLogger(__name__)

# Seed drivers list for PII masking test
DRIVERS = [
    "John Smith", "Alice Johnson", "Robert Miller", "Emma Davis", 
    "Michael Brown", "Sarah Wilson", "David Moore", "Olivia Taylor",
    "James Anderson", "Sophia Thomas"
]

def generate_taxi_records(num_records=1000, num_duplicates=100, seed=42):
    """
    Generates realistic NYC Yellow Taxi trip records with seeded duplicates, PII, and dirty data.
    """
    np.random.seed(seed)
    
    # Base datetimes starting 7 days ago to simulate rolling window
    start_date = datetime.now() - timedelta(days=8)
    
    pickup_times = [
        start_date + timedelta(seconds=int(x))
        for x in np.random.randint(0, 86400 * 7, size=num_records)
    ]
    
    durations = np.random.randint(180, 3600, size=num_records) # 3 min to 1 hour
    dropoff_times = [
        pickup_times[i] + timedelta(seconds=int(durations[i]))
        for i in range(num_records)
    ]
    
    fares = np.round(5.0 + (durations / 60.0) * 2.5 + np.random.normal(0, 2, num_records), 2)
    fares = np.maximum(fares, 2.5) # Minimum fare
    
    extra = np.random.choice([0.0, 0.5, 1.0, 2.5], size=num_records)
    tip = np.round(fares * np.random.choice([0.0, 0.10, 0.15, 0.20], size=num_records), 2)
    tolls = np.random.choice([0.0, 6.55], p=[0.9, 0.1], size=num_records)
    total_amount = fares + extra + tip + tolls + 0.80 # 0.80 surcharge
    
    df = pd.DataFrame({
        "VendorID": np.random.choice([1, 2], size=num_records),
        "tpep_pickup_datetime": pickup_times,
        "tpep_dropoff_datetime": dropoff_times,
        "passenger_count": np.random.randint(1, 6, size=num_records),
        "trip_distance": np.round((durations / 60.0) * 0.25 + np.random.normal(0, 0.5, num_records), 2),
        "PULocationID": np.random.randint(1, 264, size=num_records),
        "DOLocationID": np.random.randint(1, 264, size=num_records),
        "fare_amount": fares,
        "extra": extra,
        "mta_tax": 0.50,
        "tip_amount": tip,
        "tolls_amount": tolls,
        "improvement_surcharge": 0.30,
        "total_amount": np.round(total_amount, 2),
        "store_and_fwd_flag": np.random.choice(["N", "Y"], p=[0.98, 0.02], size=num_records),
        "driver_name": np.random.choice(DRIVERS, size=num_records)
    })
    
    # Ensure microsecond precision for Spark compatibility
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"]).astype("datetime64[us]")
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"]).astype("datetime64[us]")

    # Enforce trip distance positive
    df["trip_distance"] = df["trip_distance"].clip(lower=0.1)
    
    # 1. Inject some dirty data to test filter logic
    # Negative fare amount (5 records)
    neg_fare_indices = np.random.choice(range(num_records), size=5, replace=False)
    df.loc[neg_fare_indices, "fare_amount"] = -10.0
    
    # Invalid dropoff datetime (3 records: dropoff before pickup)
    invalid_dt_indices = np.random.choice(range(num_records), size=3, replace=False)
    for idx in invalid_dt_indices:
        df.loc[idx, "tpep_dropoff_datetime"] = df.loc[idx, "tpep_pickup_datetime"] - timedelta(minutes=15)
        
    # Invalid PULocationID/DOLocationID (2 records each)
    invalid_loc_indices = np.random.choice(range(num_records), size=4, replace=False)
    df.loc[invalid_loc_indices[:2], "PULocationID"] = -99
    df.loc[invalid_loc_indices[2:], "DOLocationID"] = 999
    
    # 2. Add Seeded Duplicates to verify deduplication
    if num_duplicates > 0:
        logger.info(f"Seeding {num_duplicates} exact duplicate records...")
        duplicate_indices = np.random.randint(0, num_records, size=num_duplicates)
        duplicates_df = df.iloc[duplicate_indices].copy()
        df = pd.concat([df, duplicates_df], ignore_index=True)
        
    return df

def generate_and_save_data():
    """Generates Batch 1 and Batch 2 Parquet files."""
    logger.info("Generating raw NYC TLC Taxi records...")
    
    # Ensure data folder exists
    data_dir = os.path.dirname(RAW_BATCH_1_PATH)
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        
    # Batch 1: 1500 base records, 300 duplicates
    batch_1 = generate_taxi_records(1500, 300, seed=101)
    batch_1.to_parquet(RAW_BATCH_1_PATH, index=False)
    logger.info(f"Saved Batch 1 ({len(batch_1)} rows) to {RAW_BATCH_1_PATH}")
    
    # Batch 2: 700 new records, 150 duplicates
    batch_2 = generate_taxi_records(700, 150, seed=202)
    batch_2.to_parquet(RAW_BATCH_2_PATH, index=False)
    logger.info(f"Saved Batch 2 ({len(batch_2)} rows) to {RAW_BATCH_2_PATH}")
    
    return len(batch_1), len(batch_2)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    generate_and_save_data()
