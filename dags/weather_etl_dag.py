from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

import platform

if platform.system() == "Windows":
    PROJECT_DIR = r"D:\Projects\incremental_etl_pipeline"
    PYTHON_BIN = os.path.join(PROJECT_DIR, "venv", "Scripts", "python.exe")
else:
    PROJECT_DIR = "/opt/airflow"
    PYTHON_BIN = "python"

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': True,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'incremental_weather_etl',
    default_args=default_args,
    description='A simple incremental ETL DAG for weather data',
    schedule_interval=timedelta(days=1), # Run daily
    catchup=False,
    tags=['weather', 'etl', 'pyspark'],
) as dag:

    # Task 1: Ingest data from the API
    # We use the python executable from the venv to ensure dependencies are found.
    ingest_task = BashOperator(
        task_id='ingest_raw_data',
        bash_command=f'cd {PROJECT_DIR} && {PYTHON_BIN} -m scripts.ingest',
    )

    # Task 2: Transform data using PySpark
    transform_task = BashOperator(
        task_id='transform_and_load_data',
        bash_command=f'cd {PROJECT_DIR} && {PYTHON_BIN} -m scripts.transform',
    )

    # Define task dependencies
    ingest_task >> transform_task
