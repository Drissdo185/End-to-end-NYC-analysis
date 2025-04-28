import os
import sys
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
import logging
import time


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


load_dotenv()

def create_minio_client():
    """Create and return a MinIO client using environment variables."""
    try:
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
        logger.info(f"Connected to MinIO at {minio_endpoint}")
        return client
    except Exception as e:
        logger.error(f"Failed to create MinIO client: {e}")
        raise

def ensure_bucket_exists(client, bucket_name):
    """Ensure the specified bucket exists in MinIO."""
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"Created bucket: {bucket_name}")
        else:
            logger.info(f"Bucket already exists: {bucket_name}")
    except S3Error as e:
        logger.error(f"Error checking/creating bucket {bucket_name}: {e}")
        raise

def upload_file_to_minio(client, bucket_name, file_path, object_name=None):
    """Upload a file to the specified MinIO bucket."""
    if object_name is None:
        object_name = os.path.basename(file_path)
    
    try:
       
        file_stat = os.stat(file_path)
        file_size = file_stat.st_size
        
        
        client.fput_object(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=file_path
        )
        logger.info(f"Successfully uploaded {file_path} to {bucket_name}/{object_name} (size: {file_size} bytes)")
        return True
    except S3Error as e:
        logger.error(f"Error uploading {file_path} to {bucket_name}/{object_name}: {e}")
        return False

def upload_directory_to_minio(client, bucket_name, directory_path, prefix=""):
    """Upload all files in a directory to MinIO."""
    success_count = 0
    error_count = 0
    start_time = time.time()
    
    
    for root, dirs, files in os.walk(directory_path):
        for file in files:
            
            file_path = os.path.join(root, file)
            
            rel_path = os.path.relpath(file_path, directory_path)
            object_name = os.path.join(prefix, rel_path).replace("\\", "/")
            
            
            if upload_file_to_minio(client, bucket_name, file_path, object_name):
                success_count += 1
            else:
                error_count += 1
    
    duration = time.time() - start_time
    logger.info(f"Upload summary: {success_count} files uploaded successfully, {error_count} errors")
    logger.info(f"Total time: {duration:.2f} seconds")
    
    return success_count, error_count

def main():
    
    try:
       
        raw_data_dir = os.getenv("RAW_DATA_DIR", "data")
        bronze_bucket = os.getenv("BRONZE_BUCKET", "bronze")
        prefix = os.getenv("BRONZE_PREFIX", "")
        
        
        if not os.path.exists(raw_data_dir):
            logger.error(f"Raw data directory not found: {raw_data_dir}")
            return
        
        
        minio_client = create_minio_client()
        ensure_bucket_exists(minio_client, bronze_bucket)
        
        
        logger.info(f"Starting upload from {raw_data_dir} to {bronze_bucket}")
        success_count, error_count = upload_directory_to_minio(
            minio_client, bronze_bucket, raw_data_dir, prefix
        )
        
       
        logger.info(f"Upload completed: {success_count} files uploaded, {error_count} errors")
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise

if __name__ == "__main__":
    main()