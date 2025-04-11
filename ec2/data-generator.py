import faker
import json
import random
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import threading
import time

load_dotenv(".env")
RDS_HOST = os.getenv("RDS_HOST")
RDS_PASSWORD = os.getenv("RDS_PASSWORD")
LAST_ORDER_ID = None

def get_rds_engine():
    return create_engine(
        f"postgresql+psycopg2://postgres:{RDS_PASSWORD}@{RDS_HOST}:5432/postgres"
    )

def get_rds_user_id(engine):
    with engine.connect() as conn:
        result = conn.execute(text('SELECT id FROM users;'))
        user_id = [row[0] for row in result]
    return user_id

def get_rds_product(engine):
    with engine.connect() as conn:
        result = conn.execute(text('SELECT id, price FROM products;'))
        product_id = [(row[0], row[1]) for row in result]
    return product_id

def generate_clickstream_data(user_id, product_id):
    event_type_enum = ['page_view', 'add_to_cart', 'product_view']
    event_type = random.choice(event_type_enum)
    clickstream_data = {
        'user_id': random.choice(user_id),
        'timestamp': datetime.now(),
        'event_type': event_type,
        'product_id': random.choice(product_id) if event_type in ['add_to_cart', 'product_view'] else None
    }
    return clickstream_data

def generate_order_data(user_id, products):
    global LAST_ORDER_ID
    if LAST_ORDER_ID is None:
        LAST_ORDER_ID = 0
    else:
        LAST_ORDER_ID = LAST_ORDER_ID + 1
    user_id = random.choice(user_id)
    order_date = datetime.now()

    numbers_of_product = random.randint(1, 10)
    order_details = []
    random_products = random.sample(products, numbers_of_product)
    total_price = 0
    for i in range(numbers_of_product):
        purchase_product = {
           'order_id': LAST_ORDER_ID,
           'product_id': random_products[i][0],
           'quantity': random.randint(1, 5),
           'unit_price': random_products[i][1]
        }
        total_price += random_products[i][1] * purchase_product['quantity']
        purchase_product_df = pd.DataFrame(purchase_product, index=[0])
        order_details.append(purchase_product_df)
    order_data = {
        'id': LAST_ORDER_ID,
        'user_id': user_id,
        'order_date': order_date,
        'total_price': total_price
    }

    order_df = pd.DataFrame(order_data, index=[0])
    order_details_df = pd.concat(order_details, ignore_index=True)
    return [order_df, order_details_df]

def clickstream_task(engine, user_id, product_id):
    while True:
        clickstream_data = generate_clickstream_data(user_id, product_id)
        print(clickstream_data)
        time.sleep(random.randint(0, 3))

def order_task(engine, user_id, products):
    while True:
        order_data = generate_order_data(user_id, products)
        order_data[0].to_sql("orders", engine, if_exists="append", index=False)
        order_data[1].to_sql("order_details", engine, if_exists="append", index=False)
        print("Order data inserted successfully.")
        time.sleep(random.randint(0, 3))

if __name__ == "__main__":
    engine = get_rds_engine()
    products = get_rds_product(engine)
    user_id = get_rds_user_id(engine)
    product_id = [product[0] for product in products]

    # Start clickstream task
    clickstream_thread = threading.Thread(target=clickstream_task, args=(engine, user_id, product_id))
    clickstream_thread.start()

    # Start order task
    order_thread = threading.Thread(target=order_task, args=(engine, user_id, products))
    order_thread.start()

