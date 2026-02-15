# Base image
FROM ubuntu:22.04

# Set environment variables to non-interactive
ENV DEBIAN_FRONTEND=noninteractive
ENV LANG C.UTF-8

# Install necessary packages
RUN apt-get update && apt-get install -y \
    curl \
    git \
    iputils-ping \
    python3 \
    pip \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install Python libraries
RUN pip install --no-cache-dir \
    jupyterlab \
    pandas \
    matplotlib \
    seaborn \
    psycopg2-binary \
    boto3 \
    influxdb-client # ▼▼▼ ここを正しいライブラリ名に修正しました ▼▼▼

# Install MinIO Client (mc)
RUN curl https://dl.min.io/client/mc/release/linux-amd64/mc \
    --create-dirs \
    -o /usr/local/bin/mc && \
    chmod +x /usr/local/bin/mc

# Set up a working directory
WORKDIR /workspace

# Expose JupyterLab port
EXPOSE 8888

# Start JupyterLab server
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--ServerApp.token=''", "--ServerApp.password=''"]