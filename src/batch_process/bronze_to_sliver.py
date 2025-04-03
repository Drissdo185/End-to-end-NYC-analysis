from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.feature import Imputer, StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
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

def preprocess_data(spark, bronze_path, silver_path):
    """
    Preprocess data from the Bronze bucket and write to the Silver bucket.

    Args:
        spark (SparkSession): Spark session.
        bronze_path (str): Path to the Bronze bucket.
        silver_path (str): Path to the Silver bucket.

    Returns:
        None
    """
    # Read data from Bronze bucket
    loan_data = spark.read.csv(bronze_path, inferSchema=True, header=True)

    # Impute missing values
    imputer = Imputer(
        inputCols=["Income", "MonthsEmployed", "NumCreditLines", "InterestRate", "LoanTerm", "DTIRatio"],
        outputCols=["Income_filled", "MonthsEmployed_filled", "NumCreditLines_filled", "InterestRate_filled", "LoanTerm_filled", "DTIRatio_filled"]
    )
    loan_data_imputed = imputer.fit(loan_data).transform(loan_data)

    # String indexing categorical columns
    string_indexers = [
        StringIndexer(inputCol=col_name, outputCol=f"{col_name}_index")
        for col_name in ["Education", "EmploymentType", "MaritalStatus", "HasMortgage", "HasDependents", "LoanPurpose", "HasCoSigner"]
    ]
    pipeline_indexers = Pipeline(stages=string_indexers)
    loan_data_indexed = pipeline_indexers.fit(loan_data_imputed).transform(loan_data_imputed)

    # One-hot encoding indexed columns
    one_hot_encoders = [
        OneHotEncoder(inputCol=f"{col_name}_index", outputCol=f"{col_name}_vec")
        for col_name in ["Education", "EmploymentType", "MaritalStatus", "HasMortgage", "HasDependents", "LoanPurpose", "HasCoSigner"]
    ]
    pipeline_encoders = Pipeline(stages=one_hot_encoders)
    loan_data_encoded = pipeline_encoders.fit(loan_data_indexed).transform(loan_data_indexed)

    # Assemble numerical features into a vector
    numerical_cols = [
        "Age", "Income_filled", "LoanAmount", "CreditScore", "MonthsEmployed_filled",
        "NumCreditLines_filled", "InterestRate_filled", "LoanTerm_filled", "DTIRatio_filled"
    ]
    assembler = VectorAssembler(inputCols=numerical_cols, outputCol="features")
    loan_data_assembled = assembler.transform(loan_data_encoded)

    # Scale numerical features
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
    scaler_model = scaler.fit(loan_data_assembled)
    loan_data_scaled = scaler_model.transform(loan_data_assembled)

    # Write preprocessed data to Silver bucket
    loan_data_scaled.repartition(1).write.parquet(silver_path, mode="overwrite")

if __name__ == "__main__":
    # Configuration
    app_name = "Bronze to Silver bucket"
    minio_username = os.getenv("MINIO_USERNAME")
    minio_password = os.getenv("MINIO_PASSWORD")
    bronze_path = "s3a://bronze/Loan_default.csv"
    silver_path = "s3a://sliver/preprocessed_loan_data"

    # Create Spark session and preprocess data
    spark = create_spark_session(app_name, minio_username, minio_password)
    preprocess_data(spark, bronze_path, silver_path)
    spark.stop()