import os
from azure.core.exceptions import AzureError
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient

load_dotenv()
datalake_service_client = DataLakeServiceClient.from_connection_string(os.getenv("BLOB_CONNECTION_STRING"))

def delete_existing_directory(file_system, upload_directory: str):
    # Check if the directory exists on blob before attempting to delete it
    try:
        file_system_client = datalake_service_client.get_file_system_client(file_system)

        file_system_client.get_directory_client(upload_directory).get_directory_properties()
        print(f"Directory {upload_directory} exists. Deleting it...")
        file_system_client.delete_directory(upload_directory)
        directory_client = file_system_client.get_directory_client(upload_directory)
        directory_client.create_directory()
    except Exception as e:
        print(f"Directory {upload_directory} does not exist or cannot be accessed. Proceeding to create it...")

def create_directory(file_system, upload_directory: str):
    file_system_client = datalake_service_client.get_file_system_client(file_system)
    directory_client = file_system_client.get_directory_client(upload_directory)
    directory_client.create_directory()

def download_session_folder(container_name: str, folder_path: str, absolute_local_path: str):
    file_system_client = datalake_service_client.get_file_system_client(container_name)
    
    try:
        os.makedirs(absolute_local_path, exist_ok=True)
        paths = file_system_client.get_paths(path=folder_path)
        
        for path in paths:
            if path.is_directory:
                continue

            local_file_path = os.path.join(absolute_local_path, os.path.basename(path.name))
            azure_file_client = file_system_client.get_file_client(path.name)
            download_session_file(azure_file_client, local_file_path)
                
    except Exception as e:
        print(f"Error downloading directory {folder_path}: {str(e)}")

def download_session_file(file_client, local_path):
    try:
        print(f"Constructing downloader for {local_path}")
        download = file_client.download_file(max_concurrency=4)

        print(f"Downloading into {local_path}")
        total_size = download.properties['size']
        downloaded_size = 0

        with open(local_path, 'wb') as f:
            stream = download.chunks()

            for chunk in stream:
                f.write(chunk)
                downloaded_size += len(chunk)

                # Print progress every 10% or after each chunk
                if total_size > 0:
                    progress = (downloaded_size / total_size) * 100
                    print(f"Downloaded {downloaded_size} of {total_size} bytes ({progress:.2f}%)")

        print(f"Download completed: {local_path}")

    except AzureError as e:
        print(f"An error occurred while downloading {local_path}: {str(e)}")