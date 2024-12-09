import argparse
import os

from src.azure_datalake import datalake_service_client
from src.information_read import read_session_information
from src.process_video import start_all_processes

CONTAINER_NAME = 'thanapat-blob-poc'

def delete_existing_directory(file_system, upload_directory: str):
    # Check if the directory exists on blob before attempting to delete it
    try:
        file_system_client = datalake_service_client.get_file_system_client(file_system)

        file_system_client.get_directory_client(upload_directory).get_directory_properties()
        print(f"Directory {upload_directory} exists. Deleting it...")
        file_system_client.delete_directory(upload_directory)
        directory_client = file_system_client.get_directory_client(upload_directory)
        directory_client.create_directory()
    except Exception as e:
        print(f"Directory {upload_directory} does not exist or cannot be accessed. Proceeding to create it...")

def main(session_folder_path, session_detected_folder_path):
    # download_folder(CONTAINER_NAME, 'Mock_Organization_xxdgg2i0rwi2t40dollje22p/records/1731413558273_ivwupr2qc5tn54j94yii0laf','./downloaded')
    # download_folder(CONTAINER_NAME, session_folder_path, './downloaded')
    session_information = read_session_information()
    recorded_user_id = session_information['recordedUserId']
    list_of_videos = session_information['videoTlocTuples']

    for video in list_of_videos:
        video_name = video['videoName']
        video_name_exclude_ext = video_name.split('.')[0]
        tloc_name = video['tlocName']
        video_recorded_time = video['videoRecordedTime']

        video_path_abs = os.path.abspath(f'./downloaded/{video_name}')
        tloc_path_abs = os.path.abspath(f'./downloaded/{tloc_name}')

        delete_existing_directory(CONTAINER_NAME, session_detected_folder_path)

        start_all_processes(video_path_abs, tloc_path_abs, CONTAINER_NAME, session_detected_folder_path, video_recorded_time, recorded_user_id, video_name_exclude_ext)
    # start_all_processes('./downloaded/1731653640358.mp4','../downloaded/1731653640358.tloc', 'thanapat-blob-poc', session_detected_folder_path, 1731653640358, "userId", video_name_exclude_ext)


if __name__ == '__main__':
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
    # main('Mock_Organization_xxdgg2i0rwi2t40dollje22p/records/1731413558273_ivwupr2qc5tn54j94yii0laf', 'Mock_Organization_xxdgg2i0rwi2t40dollje22p/detected_images/1731413558273')
