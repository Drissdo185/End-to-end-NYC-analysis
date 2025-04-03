from minio import Minio
from glob import glob
import logging
from datetime import datetime
import pandas as pd
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.helpers import load_cfg

#Configure logging 

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BronzeIngestion:
    def __init__(self, config_file="datalake/config.yaml",):
        self.cfg = load_cfg(config_file)
        self.minio_cfg = self.cfg["minio"]
        self.bronze_cfg = self.cfg["datalake"]["bronze"]
        self.source_cfg = self.cfg["source_data"]
        
        self.client = Minio(
            endpoint=self.minio_cfg["endpoint"],
            access_key=self.minio_cfg["access_key"],
            secret_key=self.minio_cfg["secret_key"],
            secure=False
        )
    
    def create_bronze_bucket(self):
        """Create a bronze bucket if it does not exist"""
        try:
            if not self.client.bucket_exists(self.bronze_cfg["bucket_name"]):
                self.client.make_bucket(self.bronze_cfg["bucket_name"])
                logger.info(f"Created bucket {self.bronze_cfg['bucket_name']}")
            else:
                logger.info(f"Bucket {self.bronze_cfg['bucket_name']} already exists")
        except Exception as e:
            logger.error(f"Error creating bucket: {str(e)}")
            raise
    
    def determine_taxi_type(self, file_path):
        """Determine the type of taxi data to ingest"""
        file_name = os.path.basename(file_path)
        
        if "yellow" in file_name:
            return "yellow"
        elif "green" in file_name:
            return "green"
        else:
            return "Unknown"
    
    
    def get_source_files(self):
        """Get list of parquet files to ingest"""
        source_path = os.path.join(self.source_cfg["folder_path"], "*.parquet")
        print(source_path)
        files = glob(source_path)
        
        if not files:
            logger.warning("No parquet files found in source data folder")
        
        return files
    
    
    def upload_to_bronze(self, file_path):
        """Upload a single file to bronze bucket with taxi type subfolder"""
        try:
            # Determine taxi type
            taxi_type = self.determine_taxi_type(file_path)
            
            file_name = os.path.basename(file_path)
            
            
            year = "unknown"
            
            if "_" in file_name and "-" in file_name:
                try:
                    # yellow_tripdata_2020-01.parquet
                    date_part = file_name.split("_")[-1].split(".")[0]
                    year = date_part.split("-")[0]
                    
                    logger.info(f"Extracted year: {year} from file name {file_name}")
                except Exception as e:
                    logger.error(f"Error extracting year from {file_name}: {str(e)}")
            
            object_name = f"{self.bronze_cfg['folder_name']}/{taxi_type}/{year}/{file_name}"
            
            self.client.fput_object(
                self.bronze_cfg["bucket_name"],
                object_name,
                file_path
            )
            
            logger.info(f"Successfully uploaded {file_name} to bronze layer at {object_name}")
            return True
        except Exception as e:
            logger.error(f"Error uploading {os.path.basename(file_path)}: {str(e)}")
            return False
    
    def process(self):
        """Main process to upload all files to bronze layer"""
        try:
            self.create_bronze_bucket()
            
            source_files = self.get_source_files()
        
            if not source_files:
                return
            
            successful_uploads = 0
            failed_uploads = 0
            taxi_types_counts = {"yellow": 0, "green": 0, "unknown": 0}
            
            
            for file_path in source_files:
                taxi_type = self.determine_taxi_type(file_path)
                taxi_types_counts[taxi_type] += 1
                
                if self.upload_to_bronze(file_path):
                    successful_uploads += 1
                else:
                    failed_uploads += 1
            
            logger.info(f"""
            Ingestion Summary:
            - Total files processed: {len(source_files)}
            - Successfully uploaded: {successful_uploads}
            - Failed uploads: {failed_uploads}
            - Yellow taxi files: {taxi_types_counts['yellow']}
            - Green taxi files: {taxi_types_counts['green']}
            - Unknown type files: {taxi_types_counts['unknown']}
            """)

        except Exception as e:
            logger.error(f"Error in bronze ingestion process: {str(e)}")
            raise

def main():
    try:
        ingestion = BronzeIngestion()
        ingestion.process()
    except Exception as e:
        logger.error(f"Bronze ingestion failed: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()