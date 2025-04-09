import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, month, year, avg, sum, count, when, unix_timestamp
from dotenv import load_dotenv
from minio import Minio

# Set environment variables
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]
load_dotenv()

def create_spark_session():
    spark = SparkSession.builder \
            .appName("SilverToGold") \
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY")) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY")) \
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{os.getenv('MINIO_ENDPOINT')}") \
            .config("spark.jars", "/home/drissdo/Desktop/End-to-end-NYC-analysis/src/jar/aws-java-sdk-bundle-1.11.901.jar, /home/drissdo/Desktop/End-to-end-NYC-analysis/src/jar/hadoop-aws-3.3.1.jar") \
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
    
    df = spark.read.parquet(f"s3a://{silver_bucket}/*")
    
    
    taxi_df = df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime"))) \
                .withColumn("pickup_day", dayofweek(col("tpep_pickup_datetime"))) \
                .withColumn("pickup_month", month(col("tpep_pickup_datetime"))) \
                .withColumn("pickup_year", year(col("tpep_pickup_datetime")))
    
    
    hourly_metrics = taxi_df.groupBy("pickup_year", "pickup_month", "pickup_day", "pickup_hour") \
                           .agg(
                               count("*").alias("trip_count"),
                               avg("trip_distance").alias("avg_distance"),
                               avg("fare_amount").alias("avg_fare"),
                               sum("fare_amount").alias("total_fare"),
                               avg("tip_amount").alias("avg_tip"),
                               avg("passenger_count").alias("avg_passengers")
                           )
    
    
    daily_metrics = taxi_df.groupBy("pickup_year", "pickup_month", "pickup_day") \
                          .agg(
                              sum("total_amount").alias("daily_revenue"),
                              sum("fare_amount").alias("fare_revenue"),
                              sum("tip_amount").alias("tip_revenue"),
                              count("*").alias("trip_count")
                          )
    
   
    hourly_metrics.write.mode("overwrite") \
                       .format("parquet") \
                       .save(f"s3a://{gold_bucket}/hourly_metrics")
    
    daily_metrics.write.mode("overwrite") \
                      .format("parquet") \
                      .save(f"s3a://{gold_bucket}/daily_metrics")

def create_location_metrics(spark, silver_bucket, gold_bucket):
    """Create location-based metrics"""
    df = spark.read.parquet(f"s3a://{silver_bucket}/*")
    
    
    location_metrics = df.groupBy("PULocationID") \
                         .agg(
                             count("*").alias("pickup_count"),
                             avg("trip_distance").alias("avg_distance"),
                             avg("fare_amount").alias("avg_fare"),
                             sum("fare_amount").alias("total_revenue")
                         )
    
    
    location_traffic = df.groupBy("PULocationID") \
                        .agg(count("*").alias("trip_count")) \
                        .orderBy(col("trip_count").desc()) \
                        .withColumn("is_high_traffic", when(col("trip_count") > 1000, True).otherwise(False))
    
    location_metrics.write.mode("overwrite") \
                         .format("parquet") \
                         .save(f"s3a://{gold_bucket}/location_metrics")
    
    location_traffic.write.mode("overwrite") \
                         .format("parquet") \
                         .save(f"s3a://{gold_bucket}/location_traffic")

def create_payment_metrics(spark, silver_bucket, gold_bucket):
    """Create payment-type metrics"""
    df = spark.read.parquet(f"s3a://{silver_bucket}/*")
    
    
    df_with_payment = df.withColumn("payment_method", 
                                when(col("payment_type") == 1, "Credit Card")
                                .when(col("payment_type") == 2, "Cash")
                                .when(col("payment_type") == 3, "No Charge")
                                .when(col("payment_type") == 4, "Dispute")
                                .otherwise("Unknown"))
    
    payment_metrics = df_with_payment.groupBy("payment_type", "payment_method") \
                       .agg(
                           count("*").alias("trip_count"),
                           avg("fare_amount").alias("avg_fare"),
                           avg("tip_amount").alias("avg_tip"),
                           sum("total_amount").alias("total_revenue")
                       )
    
    
    tip_analysis = df.filter(col("payment_type") == 1) \
                     .withColumn("tip_percentage", (col("tip_amount") / col("fare_amount")) * 100) \
                     .groupBy("pickup_hour") \
                     .agg(
                         avg("tip_percentage").alias("avg_tip_percentage"),
                         avg("tip_amount").alias("avg_tip_amount")
                     )
    
    payment_metrics.write.mode("overwrite") \
                        .format("parquet") \
                        .save(f"s3a://{gold_bucket}/payment_metrics")
    
    tip_analysis.write.mode("overwrite") \
                     .format("parquet") \
                     .save(f"s3a://{gold_bucket}/tip_analysis")

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
    
    print("Silver to Gold processing completed successfully")
    spark.stop()

if __name__ == "__main__":
    main()