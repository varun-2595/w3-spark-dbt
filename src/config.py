import os

# Project Directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
EVIDENCE_DIR = os.path.join(BASE_DIR, "docs", "evidence")

# Real NYC TLC months ingested by the pipeline (see download_tlc_data.py)
TLC_MONTHS = ["2024-01", "2024-02"]

# Data File Paths (REAL NYC TLC Yellow Taxi data)
RAW_BATCH_1_PATH = os.path.join(DATA_DIR, f"yellow_tripdata_{TLC_MONTHS[0]}.parquet")
RAW_BATCH_2_PATH = os.path.join(DATA_DIR, f"yellow_tripdata_{TLC_MONTHS[1]}.parquet")
ZONE_LOOKUP_PATH = os.path.join(DATA_DIR, "taxi_zone_lookup.csv")
