from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import sys
import os
from dotenv import load_dotenv
from minio import Minio
import json

load_dotenv()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


dag = DAG(
    'bronze_file_sensor',
    default_args=default_args,
    description='Monitor Bronze bucket for new files',
    schedule_interval='*/5 * * * *', 
    start_date=datetime(2025, 5, 1),
    catchup=False,
    tags=['datalake', 'sensor'],
)

def check_for_new_files(**kwargs):
    """Check MinIO bucket for new files since last execution"""
  
    client = Minio(
        endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minio_access_key"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minio_secret_key"),
        secure=os.getenv("MINIO_SECURE", "False").lower() == "true"
    )
    
    bronze_bucket = os.getenv("BRONZE_BUCKET", "bronze")
    
   
    objects = list(client.list_objects(bronze_bucket, recursive=True))
    
   
    ti = kwargs['ti']
    latest_processed = ti.xcom_pull(task_ids='check_for_new_files', key='latest_processed')
    
    if latest_processed is None:
        
        latest_processed = datetime.now().timestamp()
        new_files_found = len(objects) > 0
    else:
       
        latest_processed = float(latest_processed)
        new_files = [obj for obj in objects if obj.last_modified.timestamp() > latest_processed]
        new_files_found = len(new_files) > 0
        
        if new_files_found:
            print(f"Found {len(new_files)} new files in {bronze_bucket}")
   
    ti.xcom_push(key='latest_processed', value=str(datetime.now().timestamp()))
    
    return new_files_found


check_files_task = PythonOperator(
    task_id='check_for_new_files',
    python_callable=check_for_new_files,
    provide_context=True,
    dag=dag,
)

trigger_processing_task = TriggerDagRunOperator(
    task_id='trigger_bronze_to_silver_processing',
    trigger_dag_id='bronze_to_silver_processing',
    execution_date='{{ ds }}',
    reset_dag_run=True,
    wait_for_completion=False,
    poke_interval=60,
    dag=dag,
    trigger_rule='all_success',
)

check_files_task >> trigger_processing_task