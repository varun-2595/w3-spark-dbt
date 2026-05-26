import boto3
from botocore.client import Config
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def upload_lookup():
    s3 = boto3.resource(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    
    bucket = s3.Bucket('warehouse')
    local_path = os.path.join("data", "taxi_zone_lookup.csv")
    s3_key = "lookup/taxi_zone_lookup.csv"
    
    logger.info(f"Uploading {local_path} to s3://warehouse/{s3_key}...")
    bucket.upload_file(local_path, s3_key)
    logger.info("Upload completed successfully.")

if __name__ == "__main__":
    upload_lookup()
