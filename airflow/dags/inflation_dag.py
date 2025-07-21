import os
import requests
import smtplib
import pandas as pd
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from dotenv import load_dotenv
from email.mime.text import MIMEText
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

@dag(dag_id='inflation_pipeline_dag', default_args=default_args, start_date=datetime(2025, 7, 19), schedule_interval='@monthly', catchup=False)
def inflation_pipeline_dag():
    @task
    def extract_data():
        url = "https://api.worldbank.org/v2/country/ke/indicator/FP.CPI.TOTL?format=json&date=2006:2024"

        inflation_data = []
        response = requests.get(url)
        if response.status_code == 200:
            cpi_data = response.json()
            for record in cpi_data[1]:
                inflation_data.append(
                    {
                        'year': record['date'],
                        'value': record['value'],
                        'indicator': record['indicator']['value']
                    }
                )
        else:
            print(f"Error extracting CPI data: {response.status_code}, {response.text}")

        df = pd.DataFrame(inflation_data)
        return df
    
    @task
    def transform_data(df):
        df['year'] = pd.to_datetime(df['year'], format='%Y')
        return df
    
    @task
    def load_inflation_data(clean_df):
        engine = create_engine(os.getenv("PSQL_URI"))
        try:
            clean_df.to_sql('inflation_data', con=engine, schema='inflation', index=False, if_exists='append')
            print("Data loaded into inflation_data successfully!")
        except Exception as e:
            print(f"Error loading into inflation_data: {e}")

    @task
    def send_email():
        body = f"""
                Inflation pipeline DAG has run successfully, please check it out
                """
        subject = "Inflation Pipeline DAG Notification"
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
    loading = load_inflation_data(clean_df)
    email_sent = send_email()

    loading >> email_sent

inflation = inflation_pipeline_dag()