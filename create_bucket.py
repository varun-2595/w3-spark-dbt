import boto3
from botocore.client import Config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_warehouse_bucket():
    s3 = boto3.resource(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    
    bucket = s3.Bucket('warehouse')
    if bucket.creation_date:
        logger.info("Bucket 'warehouse' already exists.")
    else:
        logger.info("Creating bucket 'warehouse'...")
        bucket.create()
        logger.info("Bucket 'warehouse' created successfully.")

if __name__ == "__main__":
    create_warehouse_bucket()
