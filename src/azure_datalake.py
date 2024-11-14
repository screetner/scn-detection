import os
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient


load_dotenv()
datalake_service_client = DataLakeServiceClient.from_connection_string(os.getenv("BLOB_CONNECTION_STRING"))

def download_folder(container_name, folder_path, local_path):
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
