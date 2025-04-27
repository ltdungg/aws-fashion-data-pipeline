import boto3
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

# S3
LANDING_ZONE_BUCKET = os.getenv("LANDING_ZONE_BUCKET")
CLEAN_ZONE_BUCKET = os.getenv("CLEAN_ZONE_BUCKET")
s3_client = boto3.client('s3')

# Spark
spark = SparkSession.builder.appName("transform").getOrCreate()


def get_table_s3(bucket_name: str = None, )

def lambda_handler(event, context):
    pass