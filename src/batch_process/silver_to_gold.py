import os
import sys
import io
import time
import logging
from collections import defaultdict
import re
from dotenv import load_dotenv
import pandas as pd
import pyarrow.parquet as pq
from minio import Minio
from minio.error import S3Error
import psycopg2
from sqlalchemy import create_engine

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

load_dotenv()

def create_minio_client():
    try:
        minio_endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
        minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minio_secret_key")
        secure = os.getenv("MINIO_SECURE", "False").lower() == "true"

        client = Minio(
            endpoint=minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=secure
        )
        logger.info(f"Connected to MinIO at {minio_endpoint}")
        return client
    except Exception as e:
        logger.error(f"Failed to create MinIO client: {e}")
        raise

def create_postgres_connection():
    try:
        pg_host = os.getenv("POSTGRES_HOST", "localhost")
        pg_port = os.getenv("POSTGRES_PORT", "5432")
        pg_db = os.getenv("POSTGRES_DB", "postgres")
        pg_user = os.getenv("POSTGRES_USER", "postgres")
        pg_password = os.getenv("POSTGRES_PASSWORD", "postgres123")
        
        connection_string = f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
        engine = create_engine(connection_string)
        
        logger.info(f"Connected to PostgreSQL at {pg_host}:{pg_port}")
        return engine
    except Exception as e:
        logger.error(f"Failed to create PostgreSQL connection: {e}")
        raise

def ensure_bucket_exists(client, bucket_name):
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"Created bucket: {bucket_name}")
        else:
            logger.info(f"Bucket already exists: {bucket_name}")
    except S3Error as e:
        logger.error(f"Error checking/creating bucket {bucket_name}: {e}")
        raise

def list_folders_with_parquet_files(client, bucket_name):
    folder_files = defaultdict(list)
    
    try:
        objects = client.list_objects(bucket_name, recursive=True)
        
        for obj in objects:
            if obj.object_name.endswith('.parquet'):
                folder_name = os.path.dirname(obj.object_name)
                if folder_name:
                    folder_files[folder_name].append(obj.object_name)
        
        logger.info(f"Found {len(folder_files)} folders with Parquet files in {bucket_name} bucket")
        return folder_files
    except Exception as e:
        logger.error(f"Error listing objects in bucket {bucket_name}: {e}")
        raise

def extract_date_from_folder(folder_name):
    match = re.search(r'(\d{4}-\d{2})', folder_name)
    if match:
        return match.group(1)
    return None

def process_parquet_to_csv_and_postgres(client, source_bucket, destination_bucket, pg_engine):
    start_time = time.time()
    folders_processed = 0
    files_processed = 0
    tables_created = 0
    
    folder_files = list_folders_with_parquet_files(client, source_bucket)
    
    for folder, files in folder_files.items():
        logger.info(f"Processing folder: {folder}")
        folder_data = pd.DataFrame()
        folder_files_processed = 0
        
        for parquet_file in files:
            try:
                response = client.get_object(source_bucket, parquet_file)
                data = response.read()
                response.close()
                
                parquet_buffer = io.BytesIO(data)
                df = pq.read_table(parquet_buffer).to_pandas()
                
                folder_data = pd.concat([folder_data, df], ignore_index=True)
                
                logger.info(f"Processed: {parquet_file}")
                folder_files_processed += 1
                files_processed += 1
            except Exception as e:
                logger.error(f"Error processing {parquet_file}: {e}")
        
        csv_filename = f"{folder.replace('/', '_')}.csv"
        
        # Save to MinIO gold bucket
        try:
            csv_data = folder_data.to_csv(index=False).encode('utf-8')
            
            client.put_object(
                bucket_name=destination_bucket,
                object_name=csv_filename,
                data=io.BytesIO(csv_data),
                length=len(csv_data),
                content_type="text/csv"
            )
            
            logger.info(f"Saved {len(folder_data)} rows to {destination_bucket}/{csv_filename}")
            folders_processed += 1
        except Exception as e:
            logger.error(f"Error saving CSV for {folder}: {e}")
        
        # Save to PostgreSQL
        try:
            # Extract date (YYYY-MM) from folder name
            date_str = extract_date_from_folder(folder)
            if date_str:
                # Create table name based on data pattern e.g., yellow_tripdata_2024_01
                table_name = f"yellow_tripdata_{date_str.replace('-', '_')}"
                
                # Convert datetime columns to properly formatted timestamps
                for col in folder_data.columns:
                    if 'datetime' in col.lower() or folder_data[col].dtype == 'datetime64[ns]':
                        folder_data[col] = pd.to_datetime(folder_data[col])
                
                # Create a SQL-compatible version of the DataFrame 
                # Map PySpark/Parquet datatypes to PostgreSQL types
                folder_data.to_sql(
                    name=table_name,
                    con=pg_engine,
                    if_exists='replace',
                    index=False,
                    chunksize=10000
                )
                
                logger.info(f"Created/updated table '{table_name}' in PostgreSQL with {len(folder_data)} rows")
                tables_created += 1
            else:
                logger.warning(f"Could not extract date from folder name: {folder}")
        except Exception as e:
            logger.error(f"Error saving to PostgreSQL for {folder}: {e}")
    
    end_time = time.time()
    processing_time = end_time - start_time
    
    logger.info("=" * 50)
    logger.info(f"Processing summary:")
    logger.info(f"  - Total folders: {len(folder_files)}")
    logger.info(f"  - Folders processed: {folders_processed}")
    logger.info(f"  - Files processed: {files_processed}")
    logger.info(f"  - Tables created/updated: {tables_created}")
    logger.info(f"  - Time taken: {processing_time:.2f} seconds")
    logger.info("=" * 50)

def process_silver_to_gold():
    silver_bucket = os.getenv("SILVER_BUCKET", "silver")
    gold_bucket = os.getenv("GOLD_BUCKET", "gold")
    
    try:
        minio_client = create_minio_client()
        pg_engine = create_postgres_connection()
        
        ensure_bucket_exists(minio_client, gold_bucket)
        
        logger.info(f"Starting to process data from {silver_bucket} to {gold_bucket} bucket and PostgreSQL")
        process_parquet_to_csv_and_postgres(minio_client, silver_bucket, gold_bucket, pg_engine)
        
        logger.info(f"Successfully completed processing data from {silver_bucket} to {gold_bucket} bucket and PostgreSQL")
    except Exception as e:
        logger.error(f"An error occurred in process_silver_to_gold: {e}")
        raise

if __name__ == "__main__":
    process_silver_to_gold()