
from threading import Condition
from queue import Queue
from ultralytics import YOLO

import cv2

model = YOLO('../model/best.pt')

# detection_queue = Queue()
# detection_thread_condition = Condition()


def process_video(video_path: str):
    try:
        frame_count = 0
        video_capture = cv2.VideoCapture(video_path)

        while video_capture.isOpened():
            ret, frame = video_capture.read()

            if not ret:
                break

            frame_count += 1
            if frame_count % 10 == 0:
                results = model(frame)

                for result in results:
                    for box in result.boxes:
                        x1, y1, x2, y2 = map(int, box.xyxy[0])
                        cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 255, 0), 2)

                # with detection_thread_condition:
                #     while detection_queue.full():
                #         detection_thread_condition.wait()
                #     detection_queue.put(frame)
                #     detection_thread_condition.notify_all()

    except Exception as e:
        print(f'Error processing video: {e}')
        return None

