import threading
from queue import Queue
from threading import Condition

from src.process.config import DETECTION_QUEUE_SIZE, PROCESS_QUEUE_SIZE

assets_payload = {}

detection_queue = Queue(DETECTION_QUEUE_SIZE)
processed_assets_queue = Queue(PROCESS_QUEUE_SIZE)
detection_thread_condition = Condition()
processed_assets_thread_condition = Condition()

stop_event = threading.Event()