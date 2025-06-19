import boto3
import os
import pandas as pd
import random
import base64
import json
import awswrangler as wr

# Kinesis
KINESIS_DATA_STREAM_NAME = os.getenv("KINESIS_DATA_STREAM_NAME")
KINESIS_DATA_STREAM_ARN = os.getenv("KINESIS_DATA_STREAM_ARN")
kinesis_client = boto3.client('kinesis')

# DynamoDB
TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME")
dynamodb_client = boto3.client('dynamodb')

# Catalog
CLEAN_ZONE_DATABASE_CATALOG = os.getenv("CLEAN_ZONE_DATABASE_CATALOG")

def get_recommended_products(clean_zone_catalog: str = None, product_id: int = None):
    products_df = wr.athena.read_sql_query("SELECT * FROM products", database=clean_zone_catalog)

    product_index = products_df[products_df['product_id'] == product_id].index[0]
    sub_category = products_df.at[product_index, 'sub_category']

    recommended_products = products_df[products_df['sub_category'] == sub_category]['product_id'].to_list()

    numbers_recommend = random.randint(1, len(recommended_products) % 5)
    return random.sample(recommended_products, numbers_recommend)

def push_to_dynamodb(recommended_products: list = None, dynamo_table: str = None, user_id: int = None):
    recommended_dict = {
        'user_id': {'N': f'{user_id}'},
        'recommended_products': {'L': [{'N': f'{x}'} for x in recommended_products]}
    }

    response = dynamodb_client.put_item(
        TableName=dynamo_table,
        Item=recommended_dict
    )

    return 200


def lambda_handler(event, context):
    response = event['Records'][0]
    try:
        print(f"Processed Kinesis Event - EventID: {response['eventID']}")
        record_data = base64.b64decode(response['kinesis']['data'])
        print(f"Record Data: {record_data}")
    except Exception as e:
        print(f"An error occurred {e}")
        raise e

    data = json.loads(record_data)

    if data['event_type'] in ['add_to_cart', 'product_view']:
        product_id = data['product_id']
        user_id = data['user_id']

        recommended_products = get_recommended_products(clean_zone_catalog=CLEAN_ZONE_DATABASE_CATALOG,
                                                        product_id=product_id)

        status = push_to_dynamodb(recommended_products=recommended_products,
                                  dynamo_table=TABLE_NAME,
                                  user_id=user_id)
        if status == 200:
            return f"Successfully add recommended products for user id {user_id}"
        else:
            return f"No success add recommended for user id {user_id}!"

    else:
        return f"Page view skipped"