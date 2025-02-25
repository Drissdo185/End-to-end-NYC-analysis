from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, dayofweek, from_unixtime, unix_timestamp,
    round, when, date_format
)
import logging
import yaml

def load_cfg(cfg_file):
    """
    Load configuration from a YAML config file
    """
    cfg = None
    with open(cfg_file, "r") as f:
        try:
            cfg = yaml.safe_load(f)
        except yaml.YAMLError as exc:
            print(exc)

    return cfg

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SilverProcessor:
    def __init__(self, config_file="../config.yaml"):
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

    def read_bronze_data(self):
        """Read data from bronze layer"""
        try:
            bronze_path = f"s3a://{self.bronze_cfg['bucket_name']}/{self.bronze_cfg['folder_name']}"
            df = self.spark.read.parquet(bronze_path)
            logger.info(f"Successfully read {df.count()} records from bronze layer")
            return df
        except Exception as e:
            logger.error(f"Error reading from bronze layer: {str(e)}")
            raise

    def validate_data(self, df):
        """Perform data quality checks"""
        try:
            # Drop rows where any column contains NaN or NULL
            df_cleaned = df.dropna(how="any")  
            
            # Drop the 'ehail_fee' column
            if "ehail_fee" in df_cleaned.columns:
                df_cleaned = df_cleaned.drop("ehail_fee")
            
            # Ensure logical consistency
            df_cleaned = df_cleaned.filter(
                (col("trip_distance") >= 0) &
                (col("fare_amount") >= 0) &
                (col("total_amount") >= 0) &
                (col("lpep_dropoff_datetime") > col("lpep_pickup_datetime"))
            )
            
            total_rows = df.count()
            valid_rows = df_cleaned.count()
            logger.info(f"""
            Data Validation Summary:
            - Total records: {total_rows}
            - Valid records: {valid_rows}
            - Filtered records: {total_rows - valid_rows}
            """)
            
            return df_cleaned
        except Exception as e:
            logger.error(f"Error in data validation: {str(e)}")
            raise


    def write_to_silver(self, df):
        """Write validated data to silver layer"""
        try:
            silver_path = f"s3a://{self.silver_cfg['bucket_name']}/{self.silver_cfg['folder_name']}"
            
            df.write.mode("overwrite").parquet(silver_path)
            
            logger.info(f"Successfully wrote data to silver layer: {silver_path}")
        except Exception as e:
            logger.error(f"Error writing to silver layer: {str(e)}")
            raise

    def process(self):
        """Main processing pipeline"""
        try:
            logger.info("Starting bronze to silver processing")
            
            bronze_df = self.read_bronze_data()
            validated_df = self.validate_data(bronze_df)
            self.write_to_silver(validated_df)
            
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
