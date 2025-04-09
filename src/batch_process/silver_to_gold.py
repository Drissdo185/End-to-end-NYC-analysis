import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, month, year, avg, sum, count
from dotenv import load_dotenv
from minio import Minio

load_dotenv()

def create_spark_session():
    spark = SparkSession.builder \
            .appName("SilverToGold") \
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY")) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY")) \
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{os.getenv('MINIO_ENDPOINT')}") \
            .config("spark.jars", "/app/src/jar/aws-java-sdk-bundle-1.11.901.jar, /app/src/jar/hadoop-aws-3.3.1.jar") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
    return spark

def create_minio_client():
    minio_client = Minio(
        endpoint=os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=os.getenv("MINIO_SECURE", "False").lower() == "true"
    )
    return minio_client

def ensure_bucket_exists(client, bucket_name):
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

def create_time_dimension_metrics(spark, silver_bucket, gold_bucket):
    """Create time-based aggregated metrics"""
    # Read NYC taxi data from silver bucket
    df = spark.read.csv(f"s3a://{silver_bucket}/*.csv", header=True, inferSchema=True)
    
    # Add time dimensions
    taxi_df = df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime"))) \
                .withColumn("pickup_day", dayofweek(col("tpep_pickup_datetime"))) \
                .withColumn("pickup_month", month(col("tpep_pickup_datetime"))) \
                .withColumn("pickup_year", year(col("tpep_pickup_datetime")))
    
    # Hourly aggregations
    hourly_metrics = taxi_df.groupBy("pickup_year", "pickup_month", "pickup_day", "pickup_hour") \
                           .agg(
                               count("*").alias("trip_count"),
                               avg("trip_distance").alias("avg_distance"),
                               avg("fare_amount").alias("avg_fare"),
                               sum("fare_amount").alias("total_fare"),
                               avg("tip_amount").alias("avg_tip"),
                               avg("passenger_count").alias("avg_passengers")
                           )
    
    # Write to gold bucket
    hourly_metrics.write.mode("overwrite") \
                       .format("parquet") \
                       .save(f"s3a://{gold_bucket}/hourly_metrics")

def create_location_metrics(spark, silver_bucket, gold_bucket):
    """Create location-based metrics"""
    df = spark.read.csv(f"s3a://{silver_bucket}/*.csv", header=True, inferSchema=True)
    
    # Location aggregations
    location_metrics = df.groupBy("PULocationID") \
                         .agg(
                             count("*").alias("pickup_count"),
                             avg("trip_distance").alias("avg_distance"),
                             avg("fare_amount").alias("avg_fare"),
                             sum("fare_amount").alias("total_revenue")
                         )
    
    location_metrics.write.mode("overwrite") \
                         .format("parquet") \
                         .save(f"s3a://{gold_bucket}/location_metrics")

def create_payment_metrics(spark, silver_bucket, gold_bucket):
    """Create payment-type metrics"""
    df = spark.read.csv(f"s3a://{silver_bucket}/*.csv", header=True, inferSchema=True)
    
    payment_metrics = df.groupBy("payment_type") \
                       .agg(
                           count("*").alias("trip_count"),
                           avg("fare_amount").alias("avg_fare"),
                           avg("tip_amount").alias("avg_tip"),
                           sum("total_amount").alias("total_revenue")
                       )
    
    payment_metrics.write.mode("overwrite") \
                        .format("parquet") \
                        .save(f"s3a://{gold_bucket}/payment_metrics")

def main():
    spark = create_spark_session()
    minio_client = create_minio_client()
    
    silver_bucket = os.getenv("SILVER_BUCKET", "silver")
    gold_bucket = os.getenv("GOLD_BUCKET", "gold")
    
    # Ensure gold bucket exists
    ensure_bucket_exists(minio_client, gold_bucket)
    
    # Create gold datasets
    create_time_dimension_metrics(spark, silver_bucket, gold_bucket)
    create_location_metrics(spark, silver_bucket, gold_bucket)
    create_payment_metrics(spark, silver_bucket, gold_bucket)
    
    spark.stop()

if __name__ == "__main__":
    main()