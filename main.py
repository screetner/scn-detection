import cv2
import threading
import queue
import tempfile
from azure.storage.filedatalake import DataLakeServiceClient
from ultralytics import YOLO
import argparse
import os
from dotenv import load_dotenv
from database import get_db_connection, insert_uploaded_files

load_dotenv()

print("Initializing YOLOv8 model...")
model = YOLO('./model/best.pt')

print("Setting up Data Lake client...")
datalake_service_client = DataLakeServiceClient.from_connection_string(os.getenv("BLOB"))

detection_queue = queue.Queue(maxsize=10)
detection_condition = threading.Condition()


def download_video(file_system, directory_name, file_name):
    local_file_path = os.path.join('videos', file_name)

    # Check if the file exists locally
    if os.path.exists(local_file_path):
        print(f"File {file_name} exists locally. Using the local file.")
        with open(local_file_path, 'rb') as f:
            video_buffer = f.read()
        return video_buffer

    print(f"Starting download of {file_name} from {file_system}/{directory_name}")
    file_system_client = datalake_service_client.get_file_system_client(file_system)
    directory_client = file_system_client.get_directory_client(directory_name)
    file_client = directory_client.get_file_client(file_name)

    # Get the total size of the file
    file_properties = file_client.get_file_properties()
    total_size = file_properties.size
    print(f"Total file size: {total_size} bytes")

    # Download the entire file
    download = file_client.download_file()
    downloaded_bytes = 0
    video_buffer = b""

    for chunk in download.chunks():
        video_buffer += chunk
        downloaded_bytes += len(chunk)
        progress = (downloaded_bytes / total_size) * 100
        print(f"Download progress: {progress:.2f}%")

    print("Download complete")
    return video_buffer


def process_video(video_buffer):
    frame_count = 0
    print("Starting video processing...")

    with tempfile.NamedTemporaryFile(delete=False, suffix='.mp4') as temp_file:
        temp_file.write(video_buffer)
        temp_file_path = temp_file.name

    video_capture = cv2.VideoCapture(temp_file_path)

    fps = video_capture.get(cv2.CAP_PROP_FPS)
    total_frames = int(video_capture.get(cv2.CAP_PROP_FRAME_COUNT))
    frames_to_skip = int(fps * 5)

    print(f"Video FPS: {fps}")
    print(f"Total frames: {total_frames}")
    print(f"Processing every {frames_to_skip} frames (approximately every 5 seconds)")

    while video_capture.isOpened():
        ret, frame = video_capture.read()
        if not ret:
            break

        frame_count += 1

        if frame_count % frames_to_skip != 0:
            continue

        print(f'Processing frame {frame_count}, shape: {frame.shape}')

        results = model(frame)

        if len(results) > 0:
            print(f"Detections found in frame {frame_count}")
            for result in results:
                for box in result.boxes:
                    x1, y1, x2, y2 = map(int, box.xyxy[0])
                    cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 255, 0), 2)

            with detection_condition:
                while detection_queue.full():
                    print("Detection queue full, waiting...")
                    detection_condition.wait()
                detection_queue.put((frame_count, frame))
                detection_condition.notify()

    video_capture.release()

    import os
    os.remove(temp_file_path)

    with detection_condition:
        print("Video processing complete. Putting None in detection queue...")
        detection_queue.put(None)
        detection_condition.notify()


uploaded_files = []


def upload_detections(file_system, upload_directory):
    print(f"Starting upload to {file_system}/{upload_directory}")
    file_system_client = datalake_service_client.get_file_system_client(file_system)

    # Check if the directory exists before attempting to delete it
    try:
        file_system_client.get_directory_client(upload_directory).get_directory_properties()
        print(f"Directory {upload_directory} exists. Deleting it...")
        file_system_client.delete_directory(upload_directory)
    except Exception as e:
        print(f"Directory {upload_directory} does not exist or cannot be accessed. Proceeding to create it...")

    # Create the directory
    directory_client = file_system_client.get_directory_client(upload_directory)
    directory_client.create_directory()

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
        file_name_db = f"{upload_directory}/{file_name}"
        # file_client = directory_client.create_file(file_name)
        # file_client.upload_data(img_bytes, overwrite=True)
        print(f"Uploaded {file_name}")

        uploaded_files.append(file_name_db)

    print("Upload process complete")


def db_insert(connection, recorder_id):
    print("Inserting uploaded file names into the database...")
    insert_uploaded_files(connection, uploaded_files, recorder_id)
    print("Database insertion complete")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process and upload video detections.')
    parser.add_argument('--file_system', type=str, required=True, help='The file system name in Azure Data Lake.')
    parser.add_argument('--directory_name', type=str, required=True, help='The directory name in Azure Data Lake.')
    parser.add_argument('--file_name', type=str, required=True, help='The file name of the video to download.')
    parser.add_argument('--upload_directory', type=str, required=True, help='The directory name to upload detections.')
    parser.add_argument('--recorder_id', type=str, required=True,
                        help='The recorded user ID to associate with the detections.')

    args = parser.parse_args()

    # Download the video completely
    print("Downloading video...")
    video_buffer = download_video(args.file_system, args.directory_name, args.file_name)

    # Start processing and uploading in parallel
    print("Starting threads...")
    process_thread = threading.Thread(target=process_video, args=(video_buffer,))
    upload_thread = threading.Thread(target=upload_detections, args=(args.file_system, args.upload_directory))

    process_thread.start()
    upload_thread.start()

    print("Waiting for threads to complete...")
    process_thread.join()
    upload_thread.join()

    connection = get_db_connection()
    db_insert(connection, args.recorder_id)

    print("Video processing and uploading completed.")
