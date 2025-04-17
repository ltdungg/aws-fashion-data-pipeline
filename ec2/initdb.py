import os
import faker
import json
import random
from datetime import datetime
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os

# RDS Parameters
load_dotenv(".env")
RDS_HOST = os.getenv("RDS_HOST")
RDS_PASSWORD = os.getenv("RDS_PASSWORD")

db_params = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': RDS_PASSWORD,
    'host': RDS_HOST,
    'port': 5432
}

PRODUCT_PATH = "./product.csv"

USERS_TABLE = """
    DROP TABLE IF EXISTS users CASCADE;
    
    CREATE TABLE users (
        id INT PRIMARY KEY,
        username VARCHAR(250),
        first_name VARCHAR(250),
        last_name VARCHAR(250),
        email VARCHAR(250),
        phone VARCHAR(250),
        address VARCHAR(255),
        sex CHAR(1),
        birthdate DATE
    );
"""

PRODUCT_TABLE = """
    DROP TABLE IF EXISTS products CASCADE;
    
    CREATE TABLE IF NOT EXISTS products (
        id INT PRIMARY KEY,
        gender VARCHAR(10),
        master_category VARCHAR(250),
        sub_category VARCHAR(250),
        article_type VARCHAR(250),
        base_colour VARCHAR(250),
        season VARCHAR(250),
        year FLOAT,
        usage VARCHAR(250),
        product_display_name VARCHAR(255),
        price FLOAT
    );
"""

ORDER_TABLE = """
     DROP TABLE IF EXISTS orders CASCADE;

    CREATE TABLE IF NOT EXISTS orders (
        id INT PRIMARY KEY,
        user_id INT,
        order_date TIMESTAMP,
        total_price FLOAT,
        CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id)
    );
"""

ORDER_DETAILS_TABLE = """
    DROP TABLE IF EXISTS order_details CASCADE;
    
    CREATE TABLE IF NOT EXISTS order_details (
        order_id INT,
        product_id INT,
        quantity INT,
        unit_price FLOAT,
        PRIMARY KEY (order_id, product_id),
        CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES orders(id),
        CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES products(id)
    );
"""

def get_conn_and_cursor():
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        return conn, cursor
    except (Exception) as e:
        raise f"Error: {e}"

def create_tables(conn, cursor,table_name, table_query):
    try:
        cursor.execute(table_query)
        conn.commit()
        print(f"Table {table_name} created successfully.")
    except (Exception) as e:
        raise f"Error: {e}"

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
    # Connect to the database
    conn, cursor = get_conn_and_cursor()

    # Create tables
    create_tables(conn=conn, cursor=cursor, table_name="users", table_query=USERS_TABLE)
    create_tables(conn=conn, cursor=cursor, table_name="products", table_query=PRODUCT_TABLE)
    create_tables(conn=conn, cursor=cursor, table_name="orders", table_query=ORDER_TABLE)
    create_tables(conn=conn, cursor=cursor, table_name="order_details", table_query=ORDER_DETAILS_TABLE)

    # Generate user data
    user_data = generate_user_data(1000)

    # Insert user data into the database
    users_tuple = (tuple(row) for row in user_data.values)
    users_insert_query = """
        INSERT INTO users (id, username, first_name, last_name, email, phone, address, sex, birthdate)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(users_insert_query, users_tuple)
    conn.commit()
    print("User data inserted successfully.")


    # Read product data
    product_data = read_product_data(PRODUCT_PATH)
    # Insert product data into the database
    product_tuple = [tuple(row) for row in product_data.values]
    product_insert_query = """
        INSERT INTO products (id, gender, master_category, sub_category, article_type, base_colour, season, year, usage, product_display_name, price)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(product_insert_query, product_tuple)
    conn.commit()
    print("Product data inserted successfully.")

    # Successfully
    print("Done!")

    # Close the database connection
    cursor.close()
    conn.close()
