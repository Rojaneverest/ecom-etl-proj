# Use the official Airflow image as the base
FROM apache/airflow:2.8.1-python3.8

# Switch to root to install system dependencies
USER root
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy custom ingestion scripts into the image (for local import in DAGs)
COPY ingestion /opt/airflow/ingestion

# (DAGs, logs, plugins, config will be mounted as volumes by docker-compose)

# Set working directory
WORKDIR /opt/airflow

# Entrypoint and command are handled by the base image and docker-compose
