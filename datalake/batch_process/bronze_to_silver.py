from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, dayofweek, from_unixtime, unix_timestamp,
    round, when, date_format, lit
)
import logging
import yaml
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.helpers import load_cfg

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SilverProcessor:
    def __init__(self, config_file="datalake/config.yaml"):
        self.cfg = load_cfg(config_file)
        self.minio_cfg = self.cfg["minio"]
        self.bronze_cfg = self.cfg["datalake"]["bronze"]
        self.silver_cfg = self.cfg["datalake"]["silver"]
        
        # Initialize Spark Session
        self.spark = (SparkSession.builder
            .appName("Bronze-to-Silver-Processing")
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{self.minio_cfg['endpoint']}")
            .config("spark.hadoop.fs.s3a.access.key", self.minio_cfg["access_key"])
            .config("spark.hadoop.fs.s3a.secret.key", self.minio_cfg["secret_key"])
            .config("spark.jars", "/home/drissdo/Desktop/End-to-end-NYC-analysis/datalake/jars/aws-java-sdk-bundle-1.11.901.jar, /home/drissdo/Desktop/End-to-end-NYC-analysis/datalake/jars/hadoop-aws-3.3.1.jar") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate())
        
        logger.info("Initialized Spark session")

    def read_bronze_data(self, taxi_type):
        """Read data from bronze layer for specified taxi type"""
        try:
            
            bronze_path = f"s3a://{self.bronze_cfg['bucket_name']}/{self.bronze_cfg['folder_name']}/{taxi_type}"
            
            df = self.spark.read.parquet(bronze_path)
            
            # Standardize column names to lowercase
            df = self.standardize_column_names(df)
            
            logger.info(f"Successfully read {df.count()} {taxi_type} taxi records from bronze layer")
            return df
        except Exception as e:
            logger.error(f"Error reading {taxi_type} taxi data from bronze layer: {str(e)}")
            raise

    def standardize_column_names(self, df):
        """Convert column names to lowercase for consistency"""
        for col_name in df.columns:
            df = df.withColumnRenamed(col_name, col_name.lower())
        return df

    def validate_and_transform_green_data(self, df):
        """Validate and transform green taxi data"""
        try:
            # Drop rows where key columns contain NaN or NULL
            key_columns = ["lpep_pickup_datetime", "lpep_dropoff_datetime", "trip_distance", "fare_amount", "total_amount"]
            df_cleaned = df.dropna(subset=key_columns, how="any")
            
            # Drop the 'ehail_fee' column as requested
            if "ehail_fee" in df_cleaned.columns:
                df_cleaned = df_cleaned.drop("ehail_fee")
                logger.info("Dropped 'ehail_fee' column from green taxi data")
            
            # Ensure logical consistency
            df_cleaned = df_cleaned.filter(
                (col("trip_distance") >= 0) &
                (col("fare_amount") >= 0) &
                (col("total_amount") >= 0) &
                (col("lpep_dropoff_datetime") > col("lpep_pickup_datetime"))
            )
            
            # Standardize datetime column names
            df_cleaned = df_cleaned.withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
            df_cleaned = df_cleaned.withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")
            
            # Add taxi_type column for better data organization in silver layer
            df_cleaned = df_cleaned.withColumn("taxi_type", lit("green"))
            
            # Calculate additional metrics for silver layer
            df_cleaned = self.add_derived_columns(df_cleaned)
            
            total_rows = df.count()
            valid_rows = df_cleaned.count()
            logger.info(f"""
            Green Taxi Data Validation Summary:
            - Total records: {total_rows}
            - Valid records: {valid_rows}
            - Filtered records: {total_rows - valid_rows}
            """)
            
            return df_cleaned
        except Exception as e:
            logger.error(f"Error in green taxi data validation: {str(e)}")
            raise

    def validate_and_transform_yellow_data(self, df):
        """Validate and transform yellow taxi data"""
        try:
            # Drop rows where key columns contain NaN or NULL
            key_columns = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount", "total_amount"]
            df_cleaned = df.dropna(subset=key_columns, how="any")
            
            # Ensure logical consistency
            df_cleaned = df_cleaned.filter(
                (col("trip_distance") >= 0) &
                (col("fare_amount") >= 0) &
                (col("total_amount") >= 0) &
                (col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))
            )
            
            # Standardize datetime column names
            df_cleaned = df_cleaned.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
            df_cleaned = df_cleaned.withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
            
            # Add taxi_type column for better data organization in silver layer
            df_cleaned = df_cleaned.withColumn("taxi_type", lit("yellow"))
            
            # Calculate additional metrics for silver layer
            df_cleaned = self.add_derived_columns(df_cleaned)
            
            total_rows = df.count()
            valid_rows = df_cleaned.count()
            logger.info(f"""
            Yellow Taxi Data Validation Summary:
            - Total records: {total_rows}
            - Valid records: {valid_rows}
            - Filtered records: {total_rows - valid_rows}
            """)
            
            return df_cleaned
        except Exception as e:
            logger.error(f"Error in yellow taxi data validation: {str(e)}")
            raise

    def add_derived_columns(self, df):
        """Add derived columns for silver layer"""
        try:
            # Calculate trip duration in minutes
            df = df.withColumn(
                "trip_duration_minutes",
                round((unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60, 2)
            )
            
            # Calculate cost per mile
            df = df.withColumn(
                "cost_per_mile",
                when(col("trip_distance") > 0, round(col("total_amount") / col("trip_distance"), 2)).otherwise(0)
            )
            
            # Add day of week
            df = df.withColumn(
                "day_of_week",
                date_format(col("pickup_datetime"), "EEEE")
            )
            
            # Add hour of day
            df = df.withColumn(
                "hour_of_day",
                hour(col("pickup_datetime"))
            )
            
            return df
        except Exception as e:
            logger.error(f"Error adding derived columns: {str(e)}")
            raise

    def write_to_silver(self, df, taxi_type):
        """Write validated data to silver layer"""
        try:
            silver_path = f"s3a://{self.silver_cfg['bucket_name']}/{self.silver_cfg['folder_name']}/{taxi_type}"
            
            # Write data by partition to improve query performance
            df.write \
                .partitionBy("day_of_week", "hour_of_day") \
                .mode("overwrite") \
                .parquet(silver_path)
            
            logger.info(f"Successfully wrote {taxi_type} taxi data to silver layer: {silver_path}")
        except Exception as e:
            logger.error(f"Error writing {taxi_type} taxi data to silver layer: {str(e)}")
            raise

    def process(self):
        """Main processing pipeline"""
        try:
            logger.info("Starting bronze to silver processing")
            
            # Process green taxi data
            try:
                green_df = self.read_bronze_data("green")
                validated_green_df = self.validate_and_transform_green_data(green_df)
                self.write_to_silver(validated_green_df, "green")
                logger.info("Completed processing green taxi data")
            except Exception as e:
                logger.error(f"Error processing green taxi data: {str(e)}")
                
            # Process yellow taxi data
            try:
                yellow_df = self.read_bronze_data("yellow")
                validated_yellow_df = self.validate_and_transform_yellow_data(yellow_df)
                self.write_to_silver(validated_yellow_df, "yellow")
                logger.info("Completed processing yellow taxi data")
            except Exception as e:
                logger.error(f"Error processing yellow taxi data: {str(e)}")
            
            logger.info("Completed bronze to silver processing")
        except Exception as e:
            logger.error(f"Error in processing pipeline: {str(e)}")
            raise
        finally:
            self.spark.stop()

def main():
    try:
        processor = SilverProcessor()
        processor.process()
    except Exception as e:
        logger.error(f"Silver processing failed: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()