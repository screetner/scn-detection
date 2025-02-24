import argparse
import os
import sys

from src.azure_datalake import download_session_folder, delete_existing_directory, \
    create_directory
from src.information_read import read_session_information
from src.process_video import start_all_processes

CONTAINER_NAME = 'thanapat-blob-poc'

def main(session_folder_path, session_detected_folder_path):
    local_path_abs = os.path.abspath('./downloaded')
    download_session_folder(CONTAINER_NAME, session_folder_path, local_path_abs)

    absolute_information_path = os.path.join(local_path_abs, 'information.json')
    session_information = read_session_information(absolute_information_path)
    videoSessionId = session_information['videoSessionId']

    recorded_user_id = session_information['recordedUserId']
    list_of_videos = session_information['videoTlocTuples']

    delete_existing_directory(CONTAINER_NAME, session_detected_folder_path)
    create_directory(CONTAINER_NAME, session_detected_folder_path)

    for video in list_of_videos:
        video_name = video['videoName']
        video_name_exclude_ext = video_name.split('.')[0]
        tloc_name = video['tlocName']
        video_recorded_time = video['videoRecordedTime']

        video_path_abs = os.path.join(local_path_abs, video_name)
        tloc_path_abs = os.path.join(local_path_abs, tloc_name)

        try:
            start_all_processes(video_path_abs, tloc_path_abs, CONTAINER_NAME, session_detected_folder_path, video_recorded_time, recorded_user_id, video_name_exclude_ext, videoSessionId)
        except Exception as e:
            print(f"Error processing video {video_name}: {e}")
            raise


if __name__ == '__main__':
    try:
        # Initialize argument parser
        parser = argparse.ArgumentParser(description='Process session folder paths.')
        parser.add_argument('--session_folder_path', required=True, help='Path to the session folder')
        parser.add_argument('--session_detected_folder_path', required=True, help='Path to the session detected folder')

        # Parse arguments
        args = parser.parse_args()

        # check if downloaded folder exists
        if not os.path.exists('./downloaded'):
            os.makedirs('./downloaded')

        main(args.session_folder_path, args.session_detected_folder_path)
        sys.exit(0)

    except Exception as e:
        print(f"Error processing session folder: {e}")
        sys.exit(1)
