import cv2
import threading
import queue
import io
import tempfile
from azure.storage.filedatalake import DataLakeServiceClient
from ultralytics import YOLO

print("Initializing YOLOv8 model...")
# model = torch.load('./model/best.pt')
model = YOLO('./model/best.pt')

print("Setting up Data Lake client...")
datalake_service_client = DataLakeServiceClient.from_connection_string(
    "")

detection_queue = queue.Queue(maxsize=10)
detection_condition = threading.Condition()

def download_video(file_system, directory_name, file_name):
    print(f"Starting download of {file_name} from {file_system}/{directory_name}")
    file_system_client = datalake_service_client.get_file_system_client(file_system)
    directory_client = file_system_client.get_directory_client(directory_name)
    file_client = directory_client.get_file_client(file_name)

    # Get the total size of the file
    file_properties = file_client.get_file_properties()
    total_size = file_properties.size
    print(f"Total file size: {total_size} bytes")

    # Buffer to hold the entire video
    video_buffer = b""
    chunk_size = 10 * 1024 * 1024  # 10MB chunk size
    offset = 0

    while offset < total_size:
        print(f"Downloading chunk from offset {offset}")
        chunk = file_client.download_file(offset=offset, length=chunk_size).readall()
        video_buffer += chunk
        offset += len(chunk)

    print("Download complete")
    return video_buffer


def process_video(video_buffer):
    frame_count = 0
    print("Starting video processing...")

    # Write the video buffer to a temporary file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.mp4') as temp_file:
        temp_file.write(video_buffer)
        temp_file_path = temp_file.name

    # Open the temporary file with OpenCV
    video_capture = cv2.VideoCapture(temp_file_path)

    # Get video properties
    fps = video_capture.get(cv2.CAP_PROP_FPS)
    total_frames = int(video_capture.get(cv2.CAP_PROP_FRAME_COUNT))

    # Calculate the number of frames to skip (5 seconds worth of frames)
    frames_to_skip = int(fps * 5)

    print(f"Video FPS: {fps}")
    print(f"Total frames: {total_frames}")
    print(f"Processing every {frames_to_skip} frames (approximately every 5 seconds)")

    while video_capture.isOpened():
        ret, frame = video_capture.read()
        if not ret:
            break

        frame_count += 1

        # Process only every 5 seconds (skip frames_to_skip frames)
        if frame_count % frames_to_skip != 0:
            continue

        print(f'Processing frame {frame_count}, shape: {frame.shape}')

        # Perform object detection using YOLOv8 model
        results = model(frame)

        if len(results) > 0:  # Check if there are any detections
            print(f"Detections found in frame {frame_count}")
            with detection_condition:
                while detection_queue.full():
                    print("Detection queue full, waiting...")
                    detection_condition.wait()
                detection_queue.put((frame_count, frame))
                detection_condition.notify()

    video_capture.release()

    # Clean up the temporary file
    import os
    os.remove(temp_file_path)

    with detection_condition:
        print("Video processing complete. Putting None in detection queue...")
        detection_queue.put(None)
        detection_condition.notify()

def upload_detections(file_system, upload_directory):
    print(f"Starting upload to {file_system}/{upload_directory}")
    file_system_client = datalake_service_client.get_file_system_client(file_system)
    directory_client = file_system_client.get_directory_client(upload_directory)

    while True:
        with detection_condition:
            while detection_queue.empty():
                print("Detection queue empty, waiting for detections...")
                detection_condition.wait()
            item = detection_queue.get()
            detection_condition.notify()

        if item is None:
            print("Received end of detection signal")
            break

        frame_count, frame = item
        _, img_encoded = cv2.imencode('.jpg', frame)
        img_bytes = img_encoded.tobytes()

        print(f"Uploading detection for frame {frame_count}...")
        file_name = f"detection_{frame_count}.jpg"
        file_client = directory_client.create_file(file_name)
        file_client.upload_data(img_bytes, overwrite=True)
        print(f"Uploaded {file_name}")

    print("Upload process complete")

# Download the video completely
print("Downloading video...")
video_buffer = download_video("thanapat-blob-poc", "/", "b3025747c9fb8fb993090f369e43c007")

# Start processing and uploading in parallel
print("Starting threads...")
process_thread = threading.Thread(target=process_video, args=(video_buffer,))
upload_thread = threading.Thread(target=upload_detections, args=("thanapat-blob-poc", "test"))

process_thread.start()
upload_thread.start()

print("Waiting for threads to complete...")
process_thread.join()
upload_thread.join()

print("Video processing and uploading completed.")
