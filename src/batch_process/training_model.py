from pyspark.ml.classification import LogisticRegression
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand
import os

def create_spark_session(app_name, minio_username, minio_password):
    """
    Create and configure a Spark session for MinIO integration.

    Args:
        app_name (str): Name of the Spark application.
        minio_username (str): MinIO access key.
        minio_password (str): MinIO secret key.

    Returns:
        SparkSession: Configured Spark session.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.access.key", minio_username) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_password) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.jars", "/home/drissdo/Desktop/Scalable-Distributed-Systems/src/jars/aws-java-sdk-bundle-1.11.901.jar, /home/drissdo/Desktop/Scalable-Distributed-Systems/src/jars/hadoop-aws-3.3.1.jar") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def train_and_save_model(spark, silver_bucket_path, model_path):
    """
    Train a logistic regression model and save it to the specified path.

    Args:
        spark (SparkSession): Spark session.
        silver_bucket_path (str): Path to the Silver bucket.
        model_path (str): Path to save the trained model.

    Returns:
        None
    """
    # Read preprocessed data from Silver bucket
    loan_data_scaled = spark.read.parquet(silver_bucket_path)

    # Split data into default and non-default
    default_data = loan_data_scaled.filter(col("Default") == 1)
    non_default_data = loan_data_scaled.filter(col("Default") == 0)

    # Split into training and testing sets
    train_default, test_default = default_data.randomSplit([0.8, 0.2], seed=42)
    train_non_default, test_non_default = non_default_data.randomSplit([0.8, 0.2], seed=42)

    # Combine default and non-default data
    train_data = train_default.union(train_non_default)
    test_data = test_default.union(test_non_default)

    # Shuffle the data
    train_data = train_data.orderBy(rand(seed=42))
    test_data = test_data.orderBy(rand(seed=42))

    # Select relevant columns
    train_data = train_data.select(["LoanID", "Default", "scaled_features"])
    test_data = test_data.select(["LoanID", "scaled_features"])

    # Train logistic regression model
    lr = LogisticRegression(featuresCol="scaled_features", labelCol="Default", maxIter=10)
    lr_model = lr.fit(train_data)

    # Save the model
    if os.path.exists(model_path):
        print(f"Path {model_path} already exists. Consider removing it or choosing a new path.")
    lr_model.write().overwrite().save(model_path)

if __name__ == "__main__":
    # Configuration
    app_name = "Get data to training model"
    minio_username = os.getenv("MINIO_USERNAME")
    minio_password = os.getenv("MINIO_PASSWORD")
    silver_bucket_path = "s3a://sliver/preprocessed_loan_data"
    model_path = "/home/drissdo/Desktop/Scalable-Distributed-Systems/loan_default_lr_model"

    # Create Spark session, train model, and save it
    spark = create_spark_session(app_name, minio_username, minio_password)
    train_and_save_model(spark, silver_bucket_path, model_path)
    spark.stop()