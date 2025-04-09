import os
import sys
import time
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col

# Set environment variables
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]
load_dotenv()

def create_spark_session():
    """Create and return a configured Spark session"""
    return SparkSession.builder \
            .appName("BronzeToSilver") \
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
        print(f"Created bucket: {bucket_name}")

def list_files_in_bucket(client, bucket_name, prefix=""):
    """List all files in a bucket with optional prefix"""
    objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
    return [obj.object_name for obj in objects]

def clean_data_with_spark(spark_df):
    """Apply data cleaning operations to the dataframe"""
    return spark_df \
        .fillna(1, subset=["passenger_count"]) \
        .fillna(1, subset=["RatecodeID"]) \
        .fillna("N", subset=["store_and_fwd_flag"]) \
        .fillna(0.0, subset=["congestion_surcharge"]) \
        .fillna(0.0, subset=["Airport_fee"])

def process_file(spark, bronze_bucket, silver_bucket, file_path):
    """Process a single file from bronze to silver"""
    # Skip non-parquet files
    if not file_path.endswith('.parquet'):
        print(f"Skipping non-parquet file: {file_path}")
        return False
    
    file_name = os.path.basename(file_path)
    file_base_name = os.path.splitext(file_name)[0]
    output_path = file_base_name
    
    try:
        # Read parquet file from bronze bucket
        df = spark.read.parquet(f"s3a://{bronze_bucket}/{file_path}")
        
        # Clean the data
        cleaned_df = clean_data_with_spark(df)
        
        # Write directly to silver bucket in parquet format
        cleaned_df.write \
            .format("parquet") \
            .mode("overwrite") \
            .save(f"s3a://{silver_bucket}/{output_path}")
        
        print(f"Successfully processed: {file_path} → {output_path}")
        return True
    
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        return False

def process_bronze_to_silver():
    """Main function to process files from bronze to silver bucket"""
    # Get configuration from environment variables
    bronze_bucket = os.getenv("BRONZE_BUCKET", "bronze")
    silver_bucket = os.getenv("SILVER_BUCKET", "silver")
    
    # Setup clients
    minio_client = create_minio_client()
    ensure_bucket_exists(minio_client, silver_bucket)
    
    # Create Spark session
    spark = create_spark_session()
    
    # List files in bronze bucket
    files = list_files_in_bucket(minio_client, bronze_bucket)
    
    # Process files
    success_count = 0
    error_count = 0
    start_time = time.time()
    
    print(f"Starting to process {len(files)} files from bronze to silver bucket")
    
    for file_path in files:
        if process_file(spark, bronze_bucket, silver_bucket, file_path):
            success_count += 1
        else:
            error_count += 1
    
    # Log processing summary
    end_time = time.time()
    processing_time = end_time - start_time
    
    print("=" * 50)
    print(f"Processing summary:")
    print(f"  - Total files: {len(files)}")
    print(f"  - Successful: {success_count}")
    print(f"  - Failed: {error_count}")
    print(f"  - Time taken: {processing_time:.2f} seconds")
    print("=" * 50)
    
    # Clean up
    spark.stop()

if __name__ == "__main__":
    process_bronze_to_silver()