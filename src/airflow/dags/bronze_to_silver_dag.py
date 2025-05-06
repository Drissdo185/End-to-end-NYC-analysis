from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))
from batch_process.bronze_to_silver import process_bronze_to_silver


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bronze_to_silver_processing',
    default_args=default_args,
    description='Process data from Bronze to Silver tier',
    schedule_interval='*/10 * * * *',  
    start_date=datetime(2025, 5, 1),
    catchup=False,
    tags=['datalake', 'etl'],
)


process_task = PythonOperator(
    task_id='process_bronze_to_silver',
    python_callable=process_bronze_to_silver,
    dag=dag,
)