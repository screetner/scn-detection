# from src.tloc_decoder import read_location_binary
import threading

from src.azure_datalake import download_folder
# from src.information_read import read_session_information
from src.process_video import process_video, start_all_processes

if __name__ == '__main__':
    print("Hello World")
    # download_folder('thanapat-blob-poc', 'Mock_Organization_xxdgg2i0rwi2t40dollje22p/records/1731413558273_ivwupr2qc5tn54j94yii0laf','./downloaded')
    # session_information = read_session_information()
    start_all_processes('./downloaded/1731653640358.mp4','../downloaded/1731653640358.tloc', 'thanapat-blob-poc', 'Mock_Organization_xxdgg2i0rwi2t40dollje22p/detected_images/1731413558273', 1731653640358)
    # process_video('./downloaded/1731653640358.mp4')
    # upload_thread = threading.Thread(target=upload_detections, args=(args.file_system, args.upload_directory))
