import time
import cv2
import matplotlib.pyplot as plt
import torch
from ultralytics import YOLO

def reduce_framerate(input_video_path, output_video_path, target_fps):
    video_capture = cv2.VideoCapture(input_video_path)
    original_fps = video_capture.get(cv2.CAP_PROP_FPS)
    frame_count = int(video_capture.get(cv2.CAP_PROP_FRAME_COUNT))
    width = int(video_capture.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(video_capture.get(cv2.CAP_PROP_FRAME_HEIGHT))

    frame_interval = int(original_fps / target_fps)
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    video_writer = cv2.VideoWriter(output_video_path, fourcc, target_fps, (width, height))

    frame_index = 0
    while video_capture.isOpened():
        ret, frame = video_capture.read()
        if not ret:
            break
        if frame_index % frame_interval == 0:
            video_writer.write(frame)
        frame_index += 1

    video_capture.release()
    video_writer.release()

def track_video(model, input_video_path, output_video_path, target_fps):
    cap = cv2.VideoCapture(input_video_path)
    frame_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    frame_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    tracked_video_writer = cv2.VideoWriter(output_video_path, fourcc, target_fps, (frame_width, frame_height))

    start_time = time.time()
    results = model.track(source=input_video_path, tracker="bytetrack.yaml", conf=0.5, iou=0.2)
    # for result in results:
    #     annotated_frame = result.plot()
    #     tracked_video_writer.write(annotated_frame)
    end_time = time.time()

    cap.release()
    tracked_video_writer.release()

    return end_time - start_time

def main():
    print(torch.cuda.is_available())

    model = YOLO("model/best1.pt")
    model = model.to("cuda")
    input_video = 'downloaded/1731653640358.mp4'
    output_plot_path = 'output/tracking_time_vs_framerate.png'
    framerates = [6, 15]
    tracking_times = []

    for fps in framerates:
        output_reduced_fps_video = f'downloaded/1731653640358_reduced_{fps}fps.mp4'
        output_tracked_video = f'output/1731653640358_tracked_{fps}fps.mp4'

        reduce_framerate(input_video, output_reduced_fps_video, fps)
        tracking_time = track_video(model, output_reduced_fps_video, output_tracked_video, fps)
        tracking_times.append(tracking_time)

    plt.figure(figsize=(10, 6))
    plt.plot(framerates, tracking_times, marker='o')
    plt.title('Tracking Time vs Framerate')
    plt.xlabel('Framerate (fps)')
    plt.ylabel('Tracking Time (seconds)')
    plt.grid(True)
    plt.savefig(output_plot_path)
    plt.show()

if __name__ == "__main__":
    main()