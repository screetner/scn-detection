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
from src.custom_types.tloc_decoder import TlocTuple
from src.tloc_decoder import read_location_binary


load_dotenv()

def get_as_absolute_path(path: str):
    return os.path.join(os.path.dirname(__file__), path)

model_path = get_as_absolute_path('../model/best1.pt')
model = YOLO(model_path)

assets_payload: AssetsPayload = {}
QUEUE_SIZE = 1024
CONFIDENCE_THRESHOLD = 0.0
detection_queue = Queue(QUEUE_SIZE)
processed_assets_queue = Queue(QUEUE_SIZE)
detection_thread_condition = Condition()
processed_assets_thread_condition = Condition()

THRESHOLD_COUNTDOWN = 300  # 10 seconds

def process_task_on_queue(task, queue: Queue, condition: Condition):
    while True:
        with condition:
            while queue.empty():
                condition.wait()
            data = queue.get()
            condition.notify()

        if data is None:
            print(f"Received end of {task} signal")
            break

        task(data)

def push_to_queue_syc(data, queue: Queue, condition: Condition):
    with condition:
        while queue.full():
            condition.wait()
        queue.put(data)
        condition.notify()

def detect_frames(video_path_abs: str, initial_timestamp: int):
    try:
        video_capture = cv2.VideoCapture(video_path_abs)

        while video_capture.isOpened():
            ret, frame = video_capture.read()

            if not ret:
                break

            results = model.track(frame, persist=True, verbose=False)

            for result in results:
                if result.boxes.id is None:
                    continue

                track_ids = result.boxes.id.int().cpu().tolist()
                timestamp_ms = initial_timestamp + video_capture.get(cv2.CAP_PROP_POS_MSEC)

                for box in result.boxes:
                    x1, y1, x2, y2 = map(int, box.xyxy[0].tolist())
                    cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 255, 0), 2)

                push_to_queue_syc({
                    'frame': frame,
                    'recordedAt': timestamp_ms,
                    'trackIds': track_ids
                }, detection_queue, detection_thread_condition)

        video_capture.release()

        with detection_thread_condition:
            print("Asset detection complete. Putting None in detection queue...")
            detection_queue.put(None)
            detection_thread_condition.notify()

    except Exception as e:
        print(f'Error processing video: {e}')
        return None

def process_detections(tloc_path_abs: str):
    tloc_path_abs = get_as_absolute_path(tloc_path_abs)

    timestamp_location_queue = read_location_binary(tloc_path_abs)
    print("total tloc record count: " + str(timestamp_location_queue.qsize()))

    track_id_property_map = {}

    def process(data):
        track_ids = data["trackIds"]
        recorded_at = data["recordedAt"]
        frame = data["frame"]

        for track_id in track_ids:
            track_id_property_map.setdefault(track_id, {
                "thresholdCountdown": THRESHOLD_COUNTDOWN + 1,
                "detection": [],
            })
            track_id_property_map[track_id]["thresholdCountdown"] = THRESHOLD_COUNTDOWN + 1
            track_id_property_map[track_id]["detection"].append({
                "recordedAt": recorded_at,
                "frame": frame
            })

        processing_assets = []
        removing_keys = []

        for key, value in track_id_property_map.items():
            value["thresholdCountdown"] -= 1
            if value["thresholdCountdown"] == 0:
                processing_assets.append(value["detection"])
                removing_keys.append(key)

        for key in removing_keys:
            del track_id_property_map[key]

        if len(processing_assets) == 0:
            return

        def sort_function(detection):
            return detection[-1]['recordedAt']

        processing_assets.sort(key=sort_function)
        [process_asset(detection, timestamp_location_queue) for detection in processing_assets]

    process_task_on_queue(process, detection_queue, detection_thread_condition)

    with processed_assets_thread_condition:
        print("Asset process complete. Putting None in process queue...")
        processed_assets_queue.put(None)
        processed_assets_thread_condition.notify()

def process_asset(data, timestamp_location_queue: Queue[TlocTuple]):
    third_quartile_index = int(0.75 * (len(data) - 1))
    selected_frame = data[third_quartile_index]["frame"]
    selected_recorded_timestamp = data[-1]["recordedAt"]

    read_next = lambda q : q.queue[0]

    while not timestamp_location_queue.empty() and read_next(timestamp_location_queue)["timestamp"] > selected_recorded_timestamp:
        timestamp_location_queue.get()

    selected_tloc = read_next(timestamp_location_queue)

    push_to_queue_syc({
        'frame': selected_frame,
        'tloc': selected_tloc,
        'recordedAt': datetime.fromtimestamp(selected_recorded_timestamp / 1000, tz=timezone.utc).isoformat()
    }, processed_assets_queue, processed_assets_thread_condition)

def upload_detections(file_system, upload_directory, video_name):
    print(f"Starting upload to {file_system}/{upload_directory}")

    item_count = { 'state': 0 }
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

        item_count['state'] += 1

    process_task_on_queue(process, processed_assets_queue, processed_assets_thread_condition)

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

    detection_thread = threading.Thread(target=detect_frames, args=(video_path, initial_timestamp))
    processing_thread = threading.Thread(target=process_detections, args=(tloc_path,))
    upload_thread = threading.Thread(target=upload_detections, args=(file_system_directory, upload_directory, video_name))

    upload_thread.start()
    processing_thread.start()
    detection_thread.start()

    upload_thread.join()
    processing_thread.join()
    detection_thread.join()

    upload_assets()
