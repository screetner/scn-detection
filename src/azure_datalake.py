import os
from azure.core.exceptions import AzureError
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient

load_dotenv()
datalake_service_client = DataLakeServiceClient.from_connection_string(os.getenv("BLOB_CONNECTION_STRING"))

def download_folder(container_name, folder_path, local_path):
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

    file_system_client = datalake_service_client.get_file_system_client(container_name)
    
    try:
        os.makedirs(local_path, exist_ok=True)
        paths = file_system_client.get_paths(path=folder_path)
        
        for path in paths:
            print(f"Downloading {path.name}")
            if path.is_directory:
                continue
                
            file_client = file_system_client.get_file_client(path.name)
            local_file = os.path.join(local_path, os.path.basename(path.name))
            
            with open(local_file, 'wb') as f:
                download = file_client.download_file()
                f.write(download.readall())
                print(f"Downloaded {path.name} to {local_file}")
                
    except Exception as e:
        print(f"Error downloading directory {folder_path}: {str(e)}")
