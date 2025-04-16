import json
import logging
import pdfplumber
import os
from google.cloud import storage
from google.api_core import exceptions as gcs_exceptions
from pdfminer.pdfparser import PDFSyntaxError

# Set up logger for this module
logger = logging.getLogger(__name__)

def push_to_pinecone(message):
    try:
        # Decode and parse JSON message
        decoded_message = message.decode('utf-8')
        message_data = json.loads(decoded_message)
        logger.info(f"Processing message: {decoded_message}")

        # Extract file path from JSON
        folder_path = message_data.get('folderPath')
        file_name = message_data.get('filename')
        logger.info(f"Accessing File name: {file_name}")
        logger.info(f"Accessing File path: {folder_path}")
        if not folder_path or not file_name:
            raise ValueError("folderPath or filename missing in message")

        bucket_name = "your-bucket-name-001"

        # Construct the blob name by combining folder_path and file_name
        # Remove leading/trailing slashes from folder_path
        folder_path = folder_path.rstrip('/')
        blob_name = f"{folder_path}/{file_name}".replace('\\', '/')
        logger.info(f"Constructed blob name: {blob_name}")

        # Initialize GCP Storage client
        storage_client = storage.Client(project="gcp-dev-7125")
        
        for bucket in storage_client.list_buckets():
            print(bucket.name)
        # Get bucket and blob
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Download PDF as bytes
        pdf_bytes = blob.download_as_bytes()
        logger.info(f"Successfully fetched PDF from GCS: gs://{bucket_name}/{blob_name}")

        
        return True

    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in message: {e}")
        return False
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        return False
    except gcs_exceptions.GoogleAPIError as e:
        logger.error(f"GCP Storage error: {e}")
        return False
    except PDFSyntaxError as e:
        logger.error(f"Invalid PDF file: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error processing message: {e}")
        return False