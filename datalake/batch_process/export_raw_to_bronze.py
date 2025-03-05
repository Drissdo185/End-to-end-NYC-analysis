from minio import Minio
from glob import glob
import os 
import logging
from datetime import datetime
import pandas as pd
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
        source_path = os.path.join(self.source_cfg["path"], "*.parquet") # datalake/source_data/*.parquet
        files = glob(source_path)
        
        if not files:
            logger.warning("No parquet files found in source data folder")
        
        return files
    
    
    def upload_to_bronze(self, file_path):
        """Upload a single file to bronze bucket with taxi type subfolder"""
        try:
            # Determine taxi type
            taxi_type = self.determine_taxi_type(file_path)
            
            # Create object name with taxi type and date prefix    
        