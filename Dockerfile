# Stage 1: Build
FROM python:3.11-slim-bookworm AS build

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    libopencv-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Create and activate virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir ultralytics torch torchvision --index-url https://download.pytorch.org/whl/cpu

# Copy only necessary application files
COPY main.py .
COPY database.py .
COPY ./src/ ./src/
COPY model/best.pt ./model/
COPY model/best3.pt ./model/

# Stage 2: Runtime
FROM python:3.11-slim-bookworm

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1-mesa-glx \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Copy virtual environment from build stage
COPY --from=build /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy application files from build stage
COPY --from=build /app /app

# Set environment variables for better performance
ENV PYTHONUNBUFFERED=1
ENV OMP_NUM_THREADS=8
ENV OPENCV_OPENCL_RUNTIME=disabled
ENV ULTRALYTICS_CONFIG_DIR="/tmp/ultralytics"