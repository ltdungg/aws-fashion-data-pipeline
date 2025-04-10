import faker
import json
import random
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

RDS_HOST = "fashiondb.clm4oskymutz.ap-southeast-1.rds.amazonaws.com"
RDS_PASSWORD = "Dungluong123"

def generate_user_data(num_rows):
    fake = faker.Faker()
    profiles = []
    for _ in range(num_rows):
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
    return df

if __name__ == "__main__":
    engine = create_engine(
        f"postgresql+psycopg2://postgres:{RDS_PASSWORD}@{RDS_HOST}:5432/fashiondb"
    )

    # Generate user data
    user_data = generate_user_data(10)
    user_data.to_sql("users", engine, if_exists="replace", index=False, schema="fashion")
    print("User data inserted successfully.")

    # Read product data
    product_data = read_product_data("./products.csv")
    product_data.to_sql("products", engine, if_exists="replace", index=False, schema="fashion")
    print("Product data inserted successfully.")