import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as f
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, DateType
import datetime
import logging
import json

LANDING_BUCKET = "<YOUR_LANDING_BUCKET_NAME>"
CLEAN_BUCKET = "<YOUR_CLEAN_BUCKET_NAME>"

def read_s3_csv(bucket, key, date, schema):
    date_list = date.split("-")
    try:
        spark_df = spark.read.option("quote", "\"") \
            .option("header", "true") \
            .option("escape", '"') \
            .option("delimiter", ",") \
            .option("multiLine", "true") \
            .schema(schema) \
            .csv(f"s3://{bucket}/{key}/{date_list[0]}/{date_list[1]}/{date_list[2]}/{key}.csv")

    except Exception as e:
        logging.error(f"{key}: Error reading CSV file: {e}")
        return None

    logging.info(f"{key}: Successfully read CSV file from s3://{bucket}/{key}/{date_list[0]}/{date_list[1]}/{date_list[2]}/{key}.csv")
    return spark_df

## Glue Params
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ETL Params
logging.basicConfig(level=logging.INFO)

RUN_DATE = datetime.datetime.now().strftime("%Y-%m-%d")
RUN_DATE_LIST = RUN_DATE.split("-")

# ETL FOR USERS DATA
logging.info("Users: Starting ETL process.")
user_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("username", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("sex", StringType(), True),
    StructField("birthdate", DateType(), True),
])
users_key = 'users'

users_df = read_s3_csv(bucket=LANDING_BUCKET, key=users_key, date=RUN_DATE, schema=user_schema)

if users_df is None:
    logging.info("Users: Data not found or no new users data.")
else:
    logging.info("Users: Data found, proceeding with transformations.")

    # Transform users address
    users_address = f.split(users_df["address"], "\n")
    users_city = f.split(users_address.getItem(1), ',')
    users_country = f.split(f.trim(users_city.getItem(1)), ' ')

    users_df = users_df \
                    .withColumn("s_address", users_address.getItem(0)) \
                    .withColumn("city", users_city.getItem(0)) \
                    .withColumn("country", users_country.getItem(0)) \
                    .withColumn("zip_code", users_country.getItem(1)) \
                    .drop("address") \
                    .withColumnRenamed("s_address", "address") \
                    .withColumnRenamed("id", "user_id")

    # Write users data to clean zone
    users_df.write.mode("overwrite").parquet(f"s3://{CLEAN_BUCKET}/users/{RUN_DATE_LIST[0]}/{RUN_DATE_LIST[1]}/{RUN_DATE_LIST[2]}/")
    logging.info(f"Users: Successfully written to s3://{CLEAN_BUCKET}/users/{RUN_DATE_LIST[0]}/{RUN_DATE_LIST[1]}/{RUN_DATE_LIST[2]}/ \n")

## ETL FOR PRODUCTS DATA
logging.info("Products: Starting ETL process.")
product_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("gender", StringType(), True),
    StructField("master_category", StringType(), True),
    StructField("sub_category", StringType(), True),
    StructField("article_type", StringType(), True),
    StructField("base_colour", StringType(), True),
    StructField("season", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("usage", StringType(), True),
    StructField("product_display_name", StringType(), True)
])
products_key = 'products'
products_df = read_s3_csv(bucket=LANDING_BUCKET, key=products_key, date=RUN_DATE, schema=product_schema)
if products_df is None:
    logging.info("Products: Data not found or no new products data.")
else:
    logging.info("Products: Data found, proceeding with transformations.")

    # Transform products data
    products_df = products_df.withColumnRenamed("id", "product_id")

    # Write products data to clean zone
    products_df.write.mode("overwrite").parquet(f"s3://{CLEAN_BUCKET}/products/{RUN_DATE_LIST[0]}/{RUN_DATE_LIST[1]}/{RUN_DATE_LIST[2]}/")
    logging.info(f"Products: Successfully written to s3://{CLEAN_BUCKET}/products/{RUN_DATE_LIST[0]}/{RUN_DATE_LIST[1]}/{RUN_DATE_LIST[2]}/ \n")

# ETL FOR TRANSACTIONS DATA
logging.info("Transactions: Starting ETL process.")

order_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("order_date", DateType(), True),
    StructField("total_price", IntegerType(), True)
])
order_details_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", IntegerType(), True)
])

order_key = 'orders'
order_details_key = 'order_details'

orders_df = read_s3_csv(bucket=LANDING_BUCKET, key=order_key, date=RUN_DATE, schema=order_schema)
if orders_df is None:
    logging.info("Orders: Data not found or no new orders data.")
else:
    orders_details_df = read_s3_csv(bucket=LANDING_BUCKET, key=order_details_key, date=RUN_DATE, schema=order_details_schema)

    # Write orders data to clean zone
    orders_df.write.mode("overwrite").parquet(f"s3://{CLEAN_BUCKET}/orders/{RUN_DATE_LIST[0]}/{RUN_DATE_LIST[1]}/{RUN_DATE_LIST[2]}/")
    logging.info(f"Orders: Successfully written to s3://{CLEAN_BUCKET}/orders/{RUN_DATE_LIST[0]}/{RUN_DATE_LIST[1]}/{RUN_DATE_LIST[2]}/")
    # Write orders details data to clean zone
    orders_details_df.write.mode("overwrite").parquet(f"s3://{CLEAN_BUCKET}/orders_details/{RUN_DATE_LIST[0]}/{RUN_DATE_LIST[1]}/{RUN_DATE_LIST[2]}/")
    logging.info(f"Orders Details: Successfully written to s3://{CLEAN_BUCKET}/orders_details/{RUN_DATE_LIST[0]}/{RUN_DATE_LIST[1]}/{RUN_DATE_LIST[2]}/ \n")

# ETL FOR CLICKSTREAM DATA
logging.info("Click Streams: Starting ETL process.")

clickstreams_data = []

for i in range(1, 25):
    if i < 10:
        hour = f"0{i}"
    else:
        hour = str(i)

    try:
        clickstreams_text = spark.sparkContext.binaryFiles(f"s3://{LANDING_BUCKET}/clickstreams/{RUN_DATE_LIST[0]}/{RUN_DATE_LIST[1]}/{RUN_DATE_LIST[2]}/{hour}/*")
        if clickstreams_text.count() == 0:
            logging.info(f"Click Streams: Data found for hour {hour} on {RUN_DATE}.")
    except Exception as e:
        logging.info(f"Click Streams: Data found for hour {hour} on {RUN_DATE}.")
        continue
    else:
        logging.info(f"Click Streams: Successfully read data for hour {hour} on {RUN_DATE}.")
        for path, content in clickstreams_text.collect():
            try:
                content = content.decode('utf-8')
                content = content.split("\n")
                for line in content:
                    data = json.loads(line)
                    clickstreams_data.append(data)

            except UnicodeDecodeError:
                logging.info(f"Click Streams: File path: {path} (binary content not shown)")

if len(clickstreams_data) == 0:
    logging.info("Click Streams: No data found.")
else:
    logging.info(f"Click Streams: {len(clickstreams_data)} records found.")
    clickstreams_df = spark.createDataFrame(clickstreams_data)

    # Transform clickstreams data
    clickstreams_df = clickstreams_df.withColumn("timestamp", f.to_timestamp(clickstreams_df["timestamp"]))

    # Write clickstream data to clean zone
    clickstreams_df.write.mode("overwrite").parquet(f"s3://{CLEAN_BUCKET}/clickstreams/{RUN_DATE_LIST[0]}/{RUN_DATE_LIST[1]}/{RUN_DATE_LIST[2]}/")
    logging.info(f"Click Streams: Successfully written to s3://{CLEAN_BUCKET}/clickstreams/{RUN_DATE_LIST[0]}/{RUN_DATE_LIST[1]}/{RUN_DATE_LIST[2]}/")

job.commit()