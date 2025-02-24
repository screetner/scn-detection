import os

from dotenv import load_dotenv
from ultralytics import YOLO

load_dotenv()

def get_as_absolute_path(path: str):
    return os.path.join(os.path.dirname(__file__), path)

model_path = get_as_absolute_path('../../model/best1.pt')
model = YOLO(model_path)

DETECTION_QUEUE_SIZE = int(os.getenv('DETECTION_QUEUE_SIZE'))
PROCESS_QUEUE_SIZE = int(os.getenv('PROCESS_QUEUE_SIZE'))
FRAME_PER_OBJECT_CAP = int(os.getenv('FRAME_PER_OBJECT_CAP'))
CONFIDENCE_THRESHOLD = 0.0

THRESHOLD_COUNTDOWN = 300  # 10 seconds