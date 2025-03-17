from src.process.global_values import assets_payload
from src.process.process_detection import detect_frames
from src.process.process_processing import process_detections
from src.process.process_upload import upload_detections, upload_assets, fail_alert
from src.process.threads import ThreadWithException


def start_all_processes(video_path: str, tloc_path: str, file_system_directory: str, upload_directory: str,
                        initial_timestamp: int, recorded_user_id: str, video_name: str, video_session_id: str):
    try:
        assets_payload['recordedUserId'] = recorded_user_id
        assets_payload['assets'] = []

        detection_thread = ThreadWithException(target=detect_frames, args=(video_path, initial_timestamp))
        processing_thread = ThreadWithException(target=process_detections, args=(tloc_path,))
        upload_thread = ThreadWithException(target=upload_detections, args=(file_system_directory, upload_directory, video_name))

        detection_thread.start()
        processing_thread.start()
        upload_thread.start()

        detection_thread.join()
        processing_thread.join()
        upload_thread.join()

        upload_assets()

    except Exception as e:
        print(f"\nError processing assets: {e}")
        fail_alert(video_session_id)
        raise