import psycopg2
from datetime import datetime, timedelta
import boto3
import os
import pandas as pd

# Variables
RDS_HOST = os.getenv("RDS_HOST")
RDS_PASSWORD = os.getenv("RDS_PASSWORD")

db_params = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': RDS_PASSWORD,
    'host': RDS_HOST,
    'port': 5432
}
DATE_EXECUTION = None

# S3
RAW_BUCKET = os.getenv("S3_BUCKET")
s3 = boto3.client('s3')

def get_table_data(table_name, query):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    print(f"Getting table {table_name} data...")
    # Execute the query
    cursor.execute(query)
    # Fetch all records
    records = cursor.fetchall()
    # Get column names
    col_names = [desc[0] for desc in cursor.description]
    # Convert to DataFrame
    df = pd.DataFrame(records, columns=col_names)

    print(f"Successfully retrieved records from {table_name} table.")
    return df

def put_csv_to_s3(df, bucket_name, file_name):
    # Convert DataFrame to CSV
    csv_data = df.to_csv(index=False)
    # Upload to S3
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=csv_data)
    print(f"File {file_name} uploaded to S3 bucket {bucket_name}")

def lambda_handler(event, context):
    try:
        DATE_EXECUTION = event["DATE_EXECUTION"]
    except Exception as e:
        DATE_EXECUTION = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    # Orders Table
    table_name = "orders"
    orders_query = f"""
            SELECT * FROM orders
            WHERE order_date >= '{DATE_EXECUTION} 00:00:00'
            AND order_date <= '{DATE_EXECUTION} 23:59:59'
    """
    orders_df = get_table_data(table_name, orders_query)
    orders_file_name = f"orders/orders_{DATE_EXECUTION}.csv"
    put_csv_to_s3(orders_df, RAW_BUCKET, orders_file_name)

    # Order Details Table
    table_name = "order_details"
    order_details_query = f"""
            SELECT * FROM order_details
            WHERE order_id IN 
                (SELECT id FROM orders
                WHERE order_date >= '{DATE_EXECUTION} 00:00:00'
                AND order_date <= '{DATE_EXECUTION} 23:59:59')
    """
    order_details_df = get_table_data(table_name, order_details_query)
    order_details_file_name = f"order_details/order_details_{DATE_EXECUTION}.csv"
    put_csv_to_s3(order_details_df, RAW_BUCKET, order_details_file_name)

    # Users Table
    table_name = "users"
    users_query = f"""
            SELECT * FROM users
    """
    users_df = get_table_data(table_name, users_query)
    users_file_name = f"users/users_{DATE_EXECUTION}.csv"
    put_csv_to_s3(users_df, RAW_BUCKET, users_file_name)

    # Products Table
    table_name = "products"
    products_query = f"""
            SELECT * FROM products
    """
    products_df = get_table_data(table_name, products_query)
    products_file_name = f"products/products_{DATE_EXECUTION}.csv"
    put_csv_to_s3(products_df, RAW_BUCKET, products_file_name)


    print("All data has been successfully uploaded to S3.")