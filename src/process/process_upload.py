import gc
import os
import requests
import cv2
from src.process.global_values import assets_payload, processed_assets_queue, processed_assets_thread_condition, stop_event
from src.process.utils import process_task_on_queue, noop
from src.azure_datalake import datalake_service_client
from src.custom_types.assets_payload import Asset

def upload_detections(file_system, upload_directory, video_name):
    try:
        print(f"Starting uploading thread for {file_system}/{upload_directory}")

        item_count = {'state': 0}
        uploaded_files = []

        file_system_client = datalake_service_client.get_file_system_client(file_system)
        directory_client = file_system_client.get_directory_client(upload_directory)

        def process(data):
            frame, tloc = data['frame'], data['tloc']
            _, img_encoded = cv2.imencode('.jpg', frame)
            img_bytes = img_encoded.tobytes()

            print(f"Uploading detection for frame {item_count['state']}...")
            file_name = f"detection_{video_name}_{item_count['state']}.jpg"
            file_name_db = f"{upload_directory}/{file_name}"
            file_client = directory_client.create_file(file_name)
            file_client.upload_data(img_bytes, overwrite=True)
            print(f"Uploaded {file_name}")

            uploaded_files.append(file_name_db)

            asset: Asset = {
                "assetTypeId": "qt4ibgg4ef4r4eexzquohvfc",
                "geoCoordinate": {
                    "lat": tloc['latitude'],
                    "lng": tloc['longitude']
                },
                "imageFileName": "/" + file_name_db,
                "recordedAt": data['recordedAt'],
            }
            assets_payload['assets'].append(asset)
            item_count['state'] += 1

        process_task_on_queue(process, noop, processed_assets_queue, processed_assets_thread_condition)
        print("Upload process complete")

    except Exception as e:
        print(f"Error uploading detections: {e}")
        stop_event.set()
        raise

    finally:
        print(f'Upload Process closed')
        gc.collect()

def upload_assets():
    api_url = os.getenv('API_URL') + '/python'
    try:
        if assets_payload.get('assets'):
            print("Uploading assets...")
        requests.post(api_url, json=assets_payload)
        print("Total of assets uploaded: " + str(len(assets_payload.get('assets', []))))
    except requests.exceptions.RequestException as e:
        print(f"Error uploading assets: {e}")

def fail_alert(video_session_id: str):
    api_url = os.getenv('API_URL') + '/python/process/fail'
    try:
        requests.post(api_url, json={'videoSessionId': video_session_id})
    except requests.exceptions.RequestException as e:
        print(f"Error sending fail alert: {e}")
