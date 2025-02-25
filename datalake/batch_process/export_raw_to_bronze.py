from minio import Minio
from src.helpers import load_cfg
from glob import glob
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BronzeIngestion:
    def __init__(self, config_file="config.yaml"):
        self.cfg = load_cfg(config_file)
        self.minio_cfg = self.cfg["minio"]
        self.bronze_cfg = self.cfg["datalake"]["bronze"]
        self.source_cfg = self.cfg["source_data"]
        
        # Initialize MinIO client
        self.client = Minio(
            endpoint=self.minio_cfg["endpoint"],
            access_key=self.minio_cfg["access_key"],
            secret_key=self.minio_cfg["secret_key"],
            secure=False
        )

    def create_bronze_bucket(self):
        """Create bronze bucket if it doesn't exist"""
        try:
            if not self.client.bucket_exists(self.bronze_cfg["bucket_name"]):
                self.client.make_bucket(self.bronze_cfg["bucket_name"])
                logger.info(f'Created bronze bucket: {self.bronze_cfg["bucket_name"]}')
            else:
                logger.info(f'Bronze bucket already exists: {self.bronze_cfg["bucket_name"]}')
        except Exception as e:
            logger.error(f"Error creating bronze bucket: {str(e)}")
            raise

    def get_source_files(self):
        """Get list of parquet files from source directory"""
        source_path = os.path.join(self.source_cfg["folder_path"], "*.parquet")
        files = glob(source_path)
        if not files:
            logger.warning("No parquet files found in source directory")
        return files

    def upload_to_bronze(self, file_path):
        """Upload a single file to bronze bucket"""
        try:
            # Create object name with date prefix for better organization
            date_prefix = datetime.now().strftime("%Y/%m/%d")
            file_name = os.path.basename(file_path)
            object_name = f"{self.bronze_cfg['folder_name']}/{date_prefix}/{file_name}"

            # Upload file
            self.client.fput_object(
                bucket_name=self.bronze_cfg["bucket_name"],
                object_name=object_name,
                file_path=file_path
            )
            logger.info(f"Successfully uploaded {file_name} to bronze layer")
            return True
        except Exception as e:
            logger.error(f"Error uploading {file_name}: {str(e)}")
            return False

    def process(self):
        """Main process to upload files to bronze layer"""
        try:
            # Ensure bronze bucket exists
            self.create_bronze_bucket()

            # Get source files
            source_files = self.get_source_files()
            if not source_files:
                return

            # Upload each file
            successful_uploads = 0
            failed_uploads = 0

            for file_path in source_files:
                if self.upload_to_bronze(file_path):
                    successful_uploads += 1
                else:
                    failed_uploads += 1

            # Log summary
            logger.info(f"""
            Ingestion Summary:
            - Total files processed: {len(source_files)}
            - Successfully uploaded: {successful_uploads}
            - Failed uploads: {failed_uploads}
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