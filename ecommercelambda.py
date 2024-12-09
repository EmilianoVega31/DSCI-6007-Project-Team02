import boto3
import os
import logging

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize S3 client
s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Configuration
    source_bucket = 'team02-ecommerce-data-bucket'
    destination_bucket = 'team02-final-ecommrce-customer-segmentation'
    local_dir = '/tmp'  # Lambda's writable directory
    files_to_process = [
        'product_details.csv',
        'customer_details.csv',
        'E-commerece sales data 2024.csv'
    ]

    # Step 1: Download files from source S3 bucket
    downloaded_files = []
    for file_name in files_to_process:
        local_path = os.path.join(local_dir, file_name)
        try:
            logger.info(f"Downloading {file_name} from {source_bucket}...")
            s3.download_file(source_bucket, file_name, local_path)
            downloaded_files.append(local_path)
            logger.info(f"Downloaded {file_name} to {local_path}.")
        except Exception as e:
            logger.error(f"Failed to download {file_name}: {e}")

    # Step 2: Process the files (example: just logging their names)
    for file_path in downloaded_files:
        try:
            logger.info(f"Processing file: {file_path}")
            # Example: Read and log file content
            with open(file_path, 'r') as file:
                content = file.read()
                logger.info(f"Content of {file_path}:\n{content}")
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")

    # Step 3: Upload processed files to the destination S3 bucket
    for file_path in downloaded_files:
        file_name = os.path.basename(file_path)
        try:
            logger.info(f"Uploading {file_name} to {destination_bucket}...")
            s3.upload_file(file_path, destination_bucket, file_name)
            logger.info(f"Uploaded {file_name} to {destination_bucket}.")
        except Exception as e:
            logger.error(f"Failed to upload {file_name}: {e}")

    return {"statusCode": 200, "body": "Files processed and uploaded successfully!"}
