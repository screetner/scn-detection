
from threading import Condition
from queue import Queue


detection_queue = Queue()
detection_thread_condition = Condition()


def process_video(video_path: str):
    try:
        with open(video_path, 'rb') as video_file:
            # TODO: Process video file
            data = video_file.read()
            return data
    except FileNotFoundError:
        print(f'File not found: {video_path}')
        return None
    except Exception as e:
        print(f'Error processing video: {e}')
        return None

