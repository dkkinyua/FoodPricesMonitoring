import os
import requests
import smtplib
import pandas as pd
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from dotenv import load_dotenv
from email.mime.text import MIMEText
from io import StringIO
from sqlalchemy import create_engine

load_dotenv()

default_args = {
    'owner': 'deecodes',
    'depend_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': [os.getenv("SENDER")],
    'email_on_failure': True,
    'email_on_retry': False
}

@dag(dag_id='food_prices_pipeline_dag', default_args=default_args, start_date=datetime(2025, 7, 18), schedule_interval='@monthly', catchup=False)
def food_prices_pipeline():
    @task
    def extract_data():
        url = "https://data.humdata.org/dataset/e0d3fba6-f9a2-45d7-b949-140c455197ff/resource/517ee1bf-2437-4f8c-aa1b-cb9925b9d437/download/wfp_food_prices_ken.csv"

        response = requests.get(url)

        if response.status_code == 200:
            df = pd.read_csv(StringIO(response.text))
            return df
        else:
            print(f"Error loading data from CSV file: {response.status_code}, {response.text}")

    @task
    def transform_data(df):
        try:
            df.drop(0, axis=0, inplace=True)
            df["date"] = pd.to_datetime(df["date"])

            df["latitude"] = df["latitude"].astype(float)
            df["longitude"] = df["longitude"].astype(float)
            df["price"] = df["price"].astype(float)
            df["usdprice"] = df["usdprice"].astype(float)
            df["market_id"] = df["market_id"].astype(int)
            df["commodity_id"] = df["commodity_id"].astype(int)

            df.rename(columns={'admin1': 'province', 'admin2': 'county'}, inplace=True)

            df.dropna(axis=0, how='any', inplace=True)
            return df
        except Exception as e:
            print(f"Error loading dataframe from previous task: {e}")

    @task
    def loading_market_data(clean_df):
        try:
            market_df = clean_df[['market_id', 'province', 'county', 'latitude', 'longitude']].drop_duplicates()
            market_df.dropna(axis=0, how='any', inplace=True)
            engine = create_engine(os.getenv("PSQL_URI"))
            try:
                market_df.to_sql('dim_market', con=engine, index=False, schema='foodprices', if_exists='append')
                print("Data loaded into PostgreSQL successfully")
            except Exception as e:
                print(f"Error loading into dim_market table: {e}")
        except Exception as e:
            print(f"Error loading clean dataframe from previous task: {e}")

    @task
    def loading_comm_data(clean_df):
        try:
            comm_df = clean_df[["commodity_id", "commodity", "unit", "category"]].drop_duplicates(subset=["commodity_id"], keep='first')
            engine = create_engine(os.getenv("PSQL_URI"))
            try:
                comm_df.to_sql('dim_commodity', con=engine, schema='foodprices', index=False, if_exists='append')
                print("Data loaded successfully!")
            except Exception as e:
                print(f"Error loading into dim_commodity table: {e}")
        except Exception as e:
            print(f"Error loading clean dataframe from previous task: {e}")

    @task
    def loading_price_data(clean_df):
        try:
            price_df = clean_df[['priceflag', 'pricetype', 'currency']].drop_duplicates()
            engine = create_engine(os.getenv("PSQL_URI"))
            try:
                price_df.to_sql('dim_pricetype', con=engine, schema='foodprices', index=False, if_exists='append')
                print("Data loaded successfully!")
            except Exception as e:
                print(f"Error loading into dim_pricetype table: {e}")
        except Exception as e:
            print(f"Error loading clean dataframe from previous task: {e}")

    @task
    def loading_date_data(clean_df):
        try:
            date_df = clean_df[["date"]]
            date_df["year"] = date_df["date"].dt.year
            date_df["month"] = date_df["date"].dt.month
            date_df = date_df.drop_duplicates(subset=["date"])
            engine = create_engine(os.getenv("PSQL_URI"))
            try:
                date_df.to_sql('dim_date', con=engine, schema='foodprices', index=False, if_exists='append')
                print("Data loaded successfully!")
            except Exception as e:
                print(f"Error loading into dim_date table: {e}")
        except Exception as e:
            print(f"Error loading clean dataframe from previous task: {e}")

    @task
    def loading_fact_table(clean_df):
        try:
            engine = create_engine(os.getenv("PSQL_URI"))
            date_dim = pd.read_sql("SELECT date_id, date FROM foodprices.dim_date", con=engine)
            price_dim = pd.read_sql("SELECT pricetype_id, pricetype FROM foodprices.dim_pricetype", con=engine)

            date_dim["date"] = pd.to_datetime(date_dim["date"])

            fact_df = clean_df.copy()
            fact_df = fact_df.merge(date_dim, on='date', how='left')
            fact_df = fact_df.merge(price_dim, on='pricetype', how='left')

            fact_df = fact_df[["date_id", "market_id", "commodity_id", "pricetype_id", "price", "usdprice"]]
            try:
                fact_df.to_sql('fact_prices', con=engine, schema='foodprices', index=False, if_exists='append')
                print("Data loaded successfully!")
            except Exception as e:
                print(f"Error loading into fact_prices table: {e}")
        except Exception as e:
            print(f"Error loading clean dataframe from previous task: {e}")

    @task
    def send_email():
        body = f"""
                Food prices pipeline DAG has run successfully, please check it out
                """
        subject = "Food Prices Pipeline DAG Notification"
        sender = os.getenv("SENDER")
        receipients = [os.getenv('RECEIPIENT')]
        pwd = os.getenv("PASSWORD")

        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = sender
        msg["To"] = receipients[0]

        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp_server:
            try:
                smtp_server.login(sender, pwd)
                smtp_server.sendmail(sender, receipients, msg.as_string())
                print(f"Email sent successfully!")
            except Exception as e:
                print(f"Sending email to receipient error: {e}")

    df = extract_data()
    clean_df = transform_data(df)

    market = loading_market_data(clean_df)
    comm = loading_comm_data(clean_df)
    prices = loading_price_data(clean_df)
    dates = loading_date_data(clean_df)
    fact = loading_fact_table(clean_df)

    [market, comm, prices, dates] >> fact >> send_email()

pipeline_dag = food_prices_pipeline()



