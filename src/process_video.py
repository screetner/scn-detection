import os
import threading
import requests
from datetime import datetime, timezone
from threading import Condition
from queue import Queue

from dotenv import load_dotenv
from ultralytics import YOLO

import cv2

from src.azure_datalake import datalake_service_client
from src.custom_types.assets_payload import AssetsPayload, Asset
from src.tloc_decoder import read_location_binary


load_dotenv()

def get_as_absolute_path(path: str):
    return os.path.join(os.path.dirname(__file__), path)


model_path = get_as_absolute_path('../model/best.pt')
model = YOLO(model_path)

assets_payload: AssetsPayload = {}
QUEUE_SIZE = 1024
detection_queue = Queue(QUEUE_SIZE)
detection_thread_condition = Condition()

def process_video(video_path_abs: str, tloc_path_abs: str, initial_timestamp: int):
    try:
        tloc_path_abs = get_as_absolute_path(tloc_path_abs)

        timestamp_location_list = read_location_binary(tloc_path_abs)
        print("total tloc records: " + str(len(timestamp_location_list)))

        video_capture = cv2.VideoCapture(video_path_abs)
        fps = video_capture.get(cv2.CAP_PROP_FPS)
        real_interval_s = 2
        frame_interval = int(fps * real_interval_s)
        frame_count = 0

        while video_capture.isOpened():
            ret, frame = video_capture.read()

            if not ret:
                break

            frame_count += 1
            tloc_idx = 0
            if frame_count % frame_interval == 0:
                results = model(frame)
                has_assets = False

                for result in results:
                    for box in result.boxes:
                        has_assets = True
                        x1, y1, x2, y2 = map(int, box.xyxy[0])
                        cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 255, 0), 2)

                # Save the frame with rectangles
                if has_assets:
                    timestamp_ms = initial_timestamp + video_capture.get(cv2.CAP_PROP_POS_MSEC)
                    timestamp_location_list_len = len(timestamp_location_list)
                    tloc = timestamp_location_list[tloc_idx]

                    # Calculate frame's location using flooring
                    while tloc_idx < timestamp_location_list_len - 1 and timestamp_location_list[tloc_idx + 1][
                        'timestamp'] < timestamp_ms:
                        tloc_idx += 1
                        tloc = timestamp_location_list[tloc_idx]

                    # Push frame to detection queue
                    with detection_thread_condition:
                        while detection_queue.full():
                            detection_thread_condition.wait()
                        detection_queue.put({
                            'frame': frame,
                            'tloc': tloc,
                            'recordedAt': datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat()
                        })
                        detection_thread_condition.notify_all()

        video_capture.release()

        with detection_thread_condition:
            print("Video processing complete. Putting None in detection queue...")
            detection_queue.put(None)
            detection_thread_condition.notify()

    except Exception as e:
        print(f'Error processing video: {e}')
        return None


def upload_detections(file_system, upload_directory, video_name):
    print(f"Starting upload to {file_system}/{upload_directory}")
    file_system_client = datalake_service_client.get_file_system_client(file_system)

    # Check if the directory exists on blob before attempting to delete it
    try:
        file_system_client.get_directory_client(upload_directory).get_directory_properties()
        print(f"Directory {upload_directory} exists. Deleting it...")
        file_system_client.delete_directory(upload_directory)
    except Exception as e:
        print(f"Directory {upload_directory} does not exist or cannot be accessed. Proceeding to create it...")

    # Create the directory on blob
    directory_client = file_system_client.get_directory_client(upload_directory)
    directory_client.create_directory()

    item_count = 0
    uploaded_files = []
    while True:
        with detection_thread_condition:
            while detection_queue.empty():
                print("Detection queue empty, waiting for detections...")
                detection_thread_condition.wait()
            data = detection_queue.get()
            detection_thread_condition.notify()

        if data is None:
            print("Received end of detection signal")
            break

        frame, tloc = data['frame'], data['tloc']
        _, img_encoded = cv2.imencode('.jpg', frame)
        img_bytes = img_encoded.tobytes()

        print(f"Uploading detection for frame {item_count}...")
        file_name = f"detection_{video_name}_{item_count}.jpg"
        file_name_db = f"{upload_directory}/{file_name}"
        file_client = directory_client.create_file(file_name)
        file_client.upload_data(img_bytes, overwrite=True)
        print(f"Uploaded {file_name}")

        uploaded_files.append(file_name_db)

        # Add asset to payload
        # recordedAt =
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

        item_count += 1

    print("Upload process complete")


def upload_assets():
    api_url = os.getenv('API_URL') + '/python'

    try:
        requests.post(api_url, json=assets_payload)
    except requests.exceptions.RequestException as e:
        print(f"Error uploading assets: {e}")


def start_all_processes(video_path: str, tloc_path: str, file_system_directory: str, upload_directory: str,
                        initial_timestamp: int, recorded_user_id: str, video_name: str):
    assets_payload['recordedUserId'] = recorded_user_id
    assets_payload['assets'] = []

    process_thread = threading.Thread(target=process_video, args=(video_path, tloc_path, initial_timestamp))
    upload_thread = threading.Thread(target=upload_detections, args=(file_system_directory, upload_directory, video_name))

    upload_thread.start()
    process_thread.start()

    upload_thread.join()
    process_thread.join()

    upload_assets()
