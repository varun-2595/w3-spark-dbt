import os
import requests
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
OUTPUT_DIR = "data"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "taxi_zone_lookup.csv")

def download_file():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        
    logger.info(f"Downloading taxi zone lookup from {URL}...")
    response = requests.get(URL)
    response.raise_for_status()
    
    with open(OUTPUT_PATH, "wb") as f:
        f.write(response.content)
        
    logger.info(f"Successfully downloaded to {OUTPUT_PATH}")

if __name__ == "__main__":
    download_file()
