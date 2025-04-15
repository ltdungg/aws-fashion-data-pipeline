import os

import faker
import json
import random
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv(".env")

RDS_HOST = os.getenv("RDS_HOST")
RDS_PASSWORD = os.getenv("RDS_PASSWORD")
PRODUCT_PATH = "./product.csv"

def get_rds_engine():
    return create_engine(
        f"postgresql+psycopg2://postgres:{RDS_PASSWORD}@{RDS_HOST}:5432/postgres"
    )

def generate_user_data(num_rows):
    fake = faker.Faker()
    profiles = []
    for i in range(num_rows):
        username = fake.user_name()
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = random.choice([username+str(random.randint(0,1000)),
                               first_name+last_name+str(random.randint(0,1000)),]) + '@gmail.com'
        phone = fake.phone_number()
        address = fake.address()
        sex = fake.random_element(elements=('M', 'F'))
        birthdate = fake.date_of_birth(minimum_age=18, maximum_age=90)
        profile = {
            "id": i,
            "username": username,
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "phone": phone,
            "address": address,
            "sex": sex,
            "birthdate": birthdate
        }

        df_profile = pd.DataFrame(profile, index=[0])
        profiles.append(df_profile)

    return pd.concat(profiles, ignore_index=True)


def read_product_data(file_path):
    df = pd.read_csv(file_path)
    df = df.drop(columns=["Unnamed: 10"], axis=1)
    df = df.assign(price=1)
    df['price'] = df['price'].apply(lambda x: random.randint(1000, 2000))

    return df

if __name__ == "__main__":
    engine = get_rds_engine()

    # Generate user data
    user_data = generate_user_data(1000)
    user_data.to_sql("users", engine, if_exists="replace", index=False)
    print("User data inserted successfully.")

    # Create orders table
    orders_table = pd.DataFrame(columns=['id', 'user_id', 'order_date', 'total_price'])
    orders_table.to_sql("orders", engine, if_exists="replace", index=False)

    # Create order details table
    order_details_table = pd.DataFrame(columns=['order_id', 'product_id', 'quantity', 'unit_price'])
    order_details_table.to_sql("order_details", engine, if_exists="replace", index=False)
    print("Orders and order details tables created successfully.")

    # Read product data
    product_data = read_product_data(PRODUCT_PATH)
    product_data.to_sql("products", engine, if_exists="replace", index=False)
    print("Product data inserted successfully.")