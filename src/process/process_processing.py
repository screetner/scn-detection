import gc
from datetime import datetime, timezone

import cv2

from src.process.config import  THRESHOLD_COUNTDOWN, FRAME_PER_OBJECT_CAP, get_as_absolute_path
from src.process.global_values import detection_queue, processed_assets_queue, detection_thread_condition, \
    processed_assets_thread_condition, stop_event
from src.tloc_decoder import read_location_binary
from src.process.utils import process_task_on_queue, push_to_queue_syc


def process_asset(data, timestamp_location_queue):
    third_quartile_index = int(0.75 * (len(data) - 1))
    third_quartile_detection = data[third_quartile_index]
    selected_frame = third_quartile_detection["frame"]
    selected_box = third_quartile_detection["box"]

    x1, y1, x2, y2 = selected_box
    cv2.rectangle(selected_frame, (x1, y1), (x2, y2), (255, 0, 0), 2)

    selected_recorded_timestamp = data[-1]["recordedAt"]
    selected_recorded_timestamp_iso = datetime.fromtimestamp(
        selected_recorded_timestamp / 1000, tz=timezone.utc).isoformat()

    read_next = lambda q: q.queue[0]
    while (timestamp_location_queue.qsize() > 1) and read_next(timestamp_location_queue)["timestamp"] < selected_recorded_timestamp:
        timestamp_location_queue.get()

    selected_tloc = read_next(timestamp_location_queue)

    push_to_queue_syc({
        'frame': selected_frame,
        'tloc': selected_tloc,
        'recordedAt': selected_recorded_timestamp_iso,
    }, processed_assets_queue, processed_assets_thread_condition)


def process_detections(tloc_path_abs: str):
    try:
        # Convert tloc file path to absolute path
        tloc_path_abs = get_as_absolute_path(tloc_path_abs)
        timestamp_location_queue = read_location_binary(tloc_path_abs)
        print("total tloc record count: " + str(timestamp_location_queue.qsize()))

        if timestamp_location_queue.qsize() == 0:
            raise Exception("No tloc records found")

        track_id_property_map = {}

        def process_assets_batch(detections):
            def sort_function(detection):
                return detection[-1]['recordedAt']
            detections.sort(key=sort_function)
            [process_asset(detection, timestamp_location_queue) for detection in detections]

        def process(data):
            tracking_boxes = data["trackingBoxes"]
            recorded_at = data["recordedAt"]
            frame = data["frame"]

            for tracking_box in tracking_boxes:
                track_id = tracking_box["trackId"]
                box = tracking_box["box"]

                track_id_property_map.setdefault(track_id, {
                    "thresholdCountdown": THRESHOLD_COUNTDOWN + 1,
                    "detection": [],
                })
                track_id_property_map[track_id]["thresholdCountdown"] = THRESHOLD_COUNTDOWN + 1

                if len(track_id_property_map[track_id]["detection"]) >= FRAME_PER_OBJECT_CAP:
                    track_id_property_map[track_id]["detection"].pop(0)

                track_id_property_map[track_id]["detection"].append({
                    "recordedAt": recorded_at,
                    "frame": frame,
                    "box": box
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

            process_assets_batch(processing_assets)

        def finalizing_process():
            detections = [value["detection"] for key, value in track_id_property_map.items()]
            process_assets_batch(detections)
            track_id_property_map.clear()
            gc.collect()

        process_task_on_queue(process, finalizing_process, detection_queue, detection_thread_condition)

        with processed_assets_thread_condition:
            print("Asset process complete. Putting None in process queue...")
            push_to_queue_syc(None, processed_assets_queue, processed_assets_thread_condition)

    except Exception as e:
        print(f'Error processing asset: {e}')
        push_to_queue_syc(None, processed_assets_queue, processed_assets_thread_condition)
        stop_event.set()
        raise

    finally:
        print(f'Process Detection closed')
        gc.collect()