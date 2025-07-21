from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'dee',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def say_hello():
    print("Airflow is working as expected!")

with DAG(
    dag_id='test_dag',
    default_args=default_args,
    start_date=datetime(2025, 7, 18),
    schedule_interval='@hourly',
    catchup=False
) as dag:
    task_test = PythonOperator(
        task_id='test_task',
        python_callable=say_hello
    )

    task_test