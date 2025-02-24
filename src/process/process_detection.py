import gc

import cv2

from src.process.config import model
from src.process.global_values import detection_queue, detection_thread_condition, stop_event
from src.process.utils import push_to_queue_syc

def detect_frames(video_path_abs: str, initial_timestamp: int):
    try:
        video_capture = cv2.VideoCapture(video_path_abs)

        while video_capture.isOpened():
            if stop_event.is_set():
                print("Stop event detected in detect_frames; exiting loop.")
                break

            ret, frame = video_capture.read()
            if not ret:
                break

            results = model.track(frame, persist=True, verbose=False)

            for result in results:
                if result.boxes.id is None:
                    continue

                track_ids = result.boxes.id.int().cpu().tolist()
                timestamp_ms = initial_timestamp + video_capture.get(cv2.CAP_PROP_POS_MSEC)

                tracking_boxes = []
                for track_id, box in zip(track_ids, result.boxes):
                    x1, y1, x2, y2 = map(int, box.xyxy[0].tolist())
                    tracking_boxes.append({
                        'trackId': track_id,
                        'box': (x1, y1, x2, y2)
                    })

                push_to_queue_syc({
                    'frame': frame,
                    'recordedAt': timestamp_ms,
                    'trackingBoxes': tracking_boxes
                }, detection_queue, detection_thread_condition)

        video_capture.release()

        with detection_thread_condition:
            print("Asset detection complete. Putting None in detection queue...")
            push_to_queue_syc(None, detection_queue, detection_thread_condition)

    except Exception as e:
        print(f'Error detecting frame video: {e}')
        push_to_queue_syc(None, detection_queue, detection_thread_condition)
        stop_event.set()
        raise

    finally:
        print(f'Frame Detection closed')
        gc.collect()