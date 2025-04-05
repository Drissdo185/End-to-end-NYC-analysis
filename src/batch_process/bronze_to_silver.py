import os
import sys
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]
load_dotenv()

def create_spark_session():
    
    spark = SparkSession.builder \
            .appName("LoanDefaultPrediction") \
            .config("spark.hadoop.fs.s3a.access.key", "minio_access_key") \
            .config("spark.hadoop.fs.s3a.secret.key", "minio_secret_key") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")\
            .config("spark.jars", "/home/drissdo/Desktop/End-to-end-NYC-analysis/src/jar/aws-java-sdk-bundle-1.11.901.jar, /home/drissdo/Desktop/End-to-end-NYC-analysis/src/jar/hadoop-aws-3.3.1.jar")\
            .config("spark.hadoop.fs.s3a.path.style.access", "true")\
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
            .getOrCreate()
    
    return spark

def create_minio_client():
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
    return client

def ensure_bucket_exists(client, bucket_name):
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

def list_files_in_bucket(client, bucket_name, prefix=""):
    objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
    files = [obj.object_name for obj in objects]
    return files

def clean_data_with_spark(spark_df):
    cleaned_df = spark_df \
        .fillna(1, subset=["passenger_count"]) \
        .fillna(1, subset=["RatecodeID"]) \
        .fillna("N", subset=["store_and_fwd_flag"]) \
        .fillna(0.0, subset=["congestion_surcharge"]) \
        .fillna(0.0, subset=["Airport_fee"])
    
    return cleaned_df

def process_file(spark, minio_client, bronze_bucket, silver_bucket, file_path):
    file_name = os.path.basename(file_path)
    file_base_name = os.path.splitext(file_name)[0]
    temp_dir_name = f"{file_base_name}_temp"
    output_file_name = f"{file_base_name}.csv"
    
    if file_path.endswith('.parquet'):
        df = spark.read.parquet(f"s3a://{bronze_bucket}/{file_path}")
        
        cleaned_df = clean_data_with_spark(df)
        
        # Write to a temporary location
        cleaned_df.coalesce(1).write \
            .format("csv") \
            .option("header", "true") \
            .mode("overwrite") \
            .save(f"s3a://{bronze_bucket}/{temp_dir_name}")
        
        temp_files = list_files_in_bucket(minio_client, bronze_bucket, prefix=temp_dir_name)
        
        part_file = next((f for f in temp_files if f.endswith('.csv')), None)
        
        if part_file:
    
            local_path = f"/tmp/part_file.csv"
            minio_client.fget_object(bronze_bucket, part_file, local_path)
            
        
            minio_client.fput_object(silver_bucket, output_file_name, local_path)
            
            os.remove(local_path)
        
            for temp_file in temp_files:
                minio_client.remove_object(bronze_bucket, temp_file)
            
        return True
    else:
        return False
def main():
    bronze_bucket = os.getenv("BRONZE_BUCKET", "bronze")
    silver_bucket = os.getenv("SILVER_BUCKET", "silver")
    
    minio_client = create_minio_client()
    ensure_bucket_exists(minio_client, silver_bucket)
    
    spark = create_spark_session()
    
    files = list_files_in_bucket(minio_client, bronze_bucket)
    
    success_count = 0
    error_count = 0
    start_time = time.time()
    
    for file_path in files:
        if process_file(spark, minio_client, bronze_bucket, silver_bucket, file_path):
            success_count += 1
        else:
            error_count += 1
    
    spark.stop()

if __name__ == "__main__":
    main()