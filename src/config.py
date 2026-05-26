import os

# Project Directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

# Data File Paths
RAW_BATCH_1_PATH = os.path.join(DATA_DIR, "yellow_tripdata_2026-01.parquet")
RAW_BATCH_2_PATH = os.path.join(DATA_DIR, "yellow_tripdata_2026-02.parquet")
ZONE_LOOKUP_PATH = os.path.join(DATA_DIR, "taxi_zone_lookup.csv")
