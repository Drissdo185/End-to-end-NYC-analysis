import os
from minio import Minio
from minio.error import S3Error

def get_minio_client(endpoint, access_key, secret_key, secure=False):
    """
    Initialize and return a MinIO client.

    Args:
        endpoint (str): MinIO server endpoint (e.g., "localhost:9000").
        access_key (str): Access key for MinIO.
        secret_key (str): Secret key for MinIO.
        secure (bool): Whether to use HTTPS (default is False).

    Returns:
        Minio: MinIO client object.
    """
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)

def upload_file_to_bucket(client, bucket_name, file_path):
    """
    Upload a file to a MinIO bucket.

    Args:
        client (Minio): MinIO client object.
        bucket_name (str): Name of the bucket to upload to.
        file_path (str): Path to the file to upload.

    Returns:
        None
    """
    try:
        # Check if the bucket exists, create it if not
        if not client.bucket_exists(bucket_name):
            print(f"Bucket '{bucket_name}' does not exist. Creating it now.")
            client.make_bucket(bucket_name)

        # Check if the file exists
        if not os.path.isfile(file_path):
            print(f"The file path '{file_path}' does not exist or is not a file.")
            return

        # Upload the file
        file_name = os.path.basename(file_path)
        client.fput_object(bucket_name, file_name, file_path)
        print(f"File '{file_name}' uploaded successfully to bucket '{bucket_name}'.")

    except S3Error as e:
        print(f"S3 Error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    # Configuration
    endpoint = "localhost:9000"
    access_key = os.getenv("MINIO_USERNAME")
    secret_key = os.getenv("MINIO_PASSWORD")
    data_path = "/home/drissdo/Desktop/Scalable-Distributed-Systems/data/Loan_default.csv"
    bucket_name = "bronze"

    # Initialize MinIO client and upload file
    client = get_minio_client(endpoint, access_key, secret_key)
    upload_file_to_bucket(client, bucket_name, data_path)