from minio import Minio
from helpers import load_cfg
from glob import glob
import os

CFG_FILE = "../config.yaml"

def create_bucket_if_not_exists(client, bucket_name):
    """Create a bucket if it doesn't exist."""
    found = client.bucket_exists(bucket_name=bucket_name)
    if not found:
        client.make_bucket(bucket_name=bucket_name)
        print(f'Bucket {bucket_name} created successfully')
    else:
        print(f'Bucket {bucket_name} already exists, skip creating!')

def upload_files_to_bucket(client, bucket_name, folder_name, files):
    """Upload files to specified bucket and folder."""
    for fp in files:
        print(f"Uploading {fp} to {bucket_name}/{folder_name}")
        object_name = os.path.join(folder_name, os.path.basename(fp))
        try:
            client.fput_object(
                bucket_name=bucket_name,
                object_name=object_name,
                file_path=fp
            )
            print(f"Successfully uploaded {fp}")
        except Exception as e:
            print(f"Error uploading {fp}: {str(e)}")

def main():
    # Load configuration
    cfg = load_cfg(CFG_FILE)
    minio_cfg = cfg["minio"]
    datalake_cfg = cfg["datalake"]
    source_data_cfg = cfg["source_data"]

    # Create MinIO client
    client = Minio(
        endpoint=minio_cfg["endpoint"],
        access_key=minio_cfg["access_key"],
        secret_key=minio_cfg["secret_key"],
        secure=False,
    )

    # Get all parquet files from source
    all_files = glob(os.path.join(source_data_cfg["folder_path"], "*.parquet"))
    
    if not all_files:
        print("No parquet files found in source directory!")
        return

    # Process Bronze layer (raw data)
    if datalake_cfg["bronze"]["enabled"]:
        bronze_bucket = datalake_cfg["bronze"]["bucket_name"]
        bronze_folder = datalake_cfg["bronze"]["folder_name"]
        
        # Create bronze bucket
        create_bucket_if_not_exists(client, bronze_bucket)
        
        # Upload to bronze layer
        print("\nUploading files to bronze layer...")
        upload_files_to_bucket(client, bronze_bucket, bronze_folder, all_files)

    # Process Silver layer (processed data)
    if datalake_cfg["silver"]["enabled"]:
        silver_bucket = datalake_cfg["silver"]["bucket_name"]
        silver_folder = datalake_cfg["silver"]["folder_name"]
        
        # Create silver bucket
        create_bucket_if_not_exists(client, silver_bucket)
        
        # Note: For silver layer, you might want to add data processing logic here
        # before uploading to the silver bucket
        print("\nNote: Files in silver layer should be processed before upload.")
        print("Currently copying raw files - consider adding transformation logic.")
        
        upload_files_to_bucket(client, silver_bucket, silver_folder, all_files)

if __name__ == "__main__":
    main()