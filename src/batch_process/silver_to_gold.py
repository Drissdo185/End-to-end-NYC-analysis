import os
import sys
import time
import csv
import io
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
from pyspark.sql import SparkSession
import psycopg2
from psycopg2 import sql
import pandas as pd
import logging

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def create_spark_session():
    """Create and return a configured Spark session"""
    return SparkSession.builder \
            .appName("SilverToGold") \
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minio_access_key")) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minio_secret_key")) \
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{os.getenv('MINIO_ENDPOINT', 'localhost:9000')}") \
            .config("spark.jars", "/home/drissdo/Desktop/End-to-end-NYC-analysis/src/jar/aws-java-sdk-bundle-1.11.901.jar, /home/drissdo/Desktop/End-to-end-NYC-analysis/src/jar/hadoop-aws-3.3.1.jar") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()

def create_minio_client():
    """Create and return a configured MinIO client"""
    return Minio(
        endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minio_access_key"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minio_secret_key"),
        secure=os.getenv("MINIO_SECURE", "False").lower() == "true"
    )

def ensure_bucket_exists(client, bucket_name):
    """Create bucket if it doesn't exist"""
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        logger.info(f"Created bucket: {bucket_name}")

def list_folders_in_bucket(client, bucket_name):
    """List all folders in a bucket"""
    objects = client.list_objects(bucket_name, recursive=False)
    # MinIO doesn't have a concept of folders, so we need to infer them from object names
    folders = set()
    
    for obj in objects:
        # If the object name has a slash, it's in a "folder"
        if '/' in obj.object_name:
            folder_name = obj.object_name.split('/')[0]
            folders.add(folder_name)
    
    return list(folders)

def list_files_in_folder(client, bucket_name, folder_name):
    """List all files in a folder within a bucket"""
    objects = client.list_objects(bucket_name, prefix=f"{folder_name}/", recursive=True)
    return [obj.object_name for obj in objects]

def create_postgres_connection():
    """Create and return a connection to PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "postgres123"),
            database=os.getenv("POSTGRES_DB", "postgres")
        )
        logger.info("Successfully connected to PostgreSQL")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        raise

def save_df_to_postgres(df, table_name, conn):
    """Save a DataFrame to PostgreSQL"""
    try:
        # Create a cursor
        cursor = conn.cursor()
        
        # Convert DataFrame column names to lowercase to avoid issues with PostgreSQL
        df.columns = [col.lower() for col in df.columns]
        
        # Create table if it doesn't exist
        # Generate column definitions from DataFrame
        columns_def = []
        for col_name, dtype in zip(df.columns, df.dtypes):
            pg_type = "VARCHAR"
            if "int" in str(dtype):
                pg_type = "INTEGER"
            elif "float" in str(dtype):
                pg_type = "FLOAT"
            elif "datetime" in str(dtype):
                pg_type = "TIMESTAMP"
            elif "bool" in str(dtype):
                pg_type = "BOOLEAN"
            
            columns_def.append(f"\"{col_name}\" {pg_type}")
        
        create_table_query = sql.SQL(f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns_def)})")
        cursor.execute(create_table_query)
        
        # Use StringIO to create a file-like object for CSV
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, header=False, quoting=csv.QUOTE_MINIMAL)
        csv_buffer.seek(0)
        
        # Use COPY command for efficient data loading
        cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV", csv_buffer)
        
        # Commit the transaction
        conn.commit()
        logger.info(f"Successfully saved data to PostgreSQL table: {table_name}")
        
        # Close cursor
        cursor.close()
        
    except Exception as e:
        logger.error(f"Error saving data to PostgreSQL: {e}")
        conn.rollback()
        raise

def process_folder(spark, minio_client, silver_bucket, gold_bucket, folder_name, postgres_conn):
    """Process a folder from silver to gold"""
    logger.info(f"Processing folder: {folder_name}")
    
    try:
        # Get all files in the folder
        files = list_files_in_folder(minio_client, silver_bucket, folder_name)
        
        if not files:
            logger.warning(f"No files found in folder: {folder_name}")
            return False
        
        # Read all parquet files in the folder
        df = spark.read.parquet(f"s3a://{silver_bucket}/{folder_name}")
        
        # Clean table name (remove special characters that might cause issues in database)
        table_name = folder_name.replace('-', '_')
        
        # Convert to pandas for easier CSV handling
        pandas_df = df.toPandas()
        
        # Save to CSV in memory
        csv_buffer = io.BytesIO()
        pandas_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        
        # Upload CSV to gold bucket
        csv_file_name = f"{folder_name}.csv"
        minio_client.put_object(
            bucket_name=gold_bucket,
            object_name=csv_file_name,
            data=csv_buffer,
            length=len(csv_buffer.getvalue()),
            content_type="text/csv"
        )
        
        logger.info(f"Saved {csv_file_name} to gold bucket")
        
        # Save to PostgreSQL
        save_df_to_postgres(pandas_df, table_name, postgres_conn)
        
        return True
        
    except Exception as e:
        logger.error(f"Error processing folder {folder_name}: {e}")
        return False

def process_silver_to_gold():
    """Main function to process data from silver to gold"""
    # Get configuration from environment variables
    silver_bucket = os.getenv("SILVER_BUCKET", "silver")
    gold_bucket = os.getenv("GOLD_BUCKET", "gold")
    
    # Setup clients
    minio_client = create_minio_client()
    ensure_bucket_exists(minio_client, gold_bucket)
    
    # Create Spark session
    spark = create_spark_session()
    
    # Create PostgreSQL connection
    postgres_conn = create_postgres_connection()
    
    try:
        # List folders in silver bucket
        folders = list_folders_in_bucket(minio_client, silver_bucket)
        
        # Process folders
        success_count = 0
        error_count = 0
        start_time = time.time()
        
        logger.info(f"Starting to process {len(folders)} folders from silver to gold bucket")
        
        for folder in folders:
            if process_folder(spark, minio_client, silver_bucket, gold_bucket, folder, postgres_conn):
                success_count += 1
            else:
                error_count += 1
        
        # Log processing summary
        end_time = time.time()
        processing_time = end_time - start_time
        
        logger.info("=" * 50)
        logger.info(f"Processing summary:")
        logger.info(f"  - Total folders: {len(folders)}")
        logger.info(f"  - Successful: {success_count}")
        logger.info(f"  - Failed: {error_count}")
        logger.info(f"  - Time taken: {processing_time:.2f} seconds")
        logger.info("=" * 50)
    
    finally:
        # Close PostgreSQL connection
        if postgres_conn:
            postgres_conn.close()
        
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    process_silver_to_gold()