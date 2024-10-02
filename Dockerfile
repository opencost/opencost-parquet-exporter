# Stage 1: Builder
FROM python:3.12-bookworm AS builder

# Set environment variables for paths to avoid repetition
ENV APP_DIR=/app \
    VENV_DIR=/app/.venv

# Update system and install required packages in a single RUN command to reduce layers
RUN apt-get update && apt-get -y upgrade && apt-get install -y --no-install-recommends \
    cmake \
    build-essential \
    libatlas-base-dev \
    gfortran \
    libssl-dev \
    libffi-dev \
    libxml2-dev \
    libxslt1-dev \
    zlib1g-dev \
    libcurl4-openssl-dev \
    libboost-all-dev \
    libprotobuf-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p $APP_DIR \
    && python3 -m venv $VENV_DIR

# Copy the requirements file first to leverage Docker caching in case dependencies don't change
COPY requirements.txt $APP_DIR/

# Activate virtual environment and install Python dependencies
# Use the full path for activation
RUN /bin/bash -c "source $VENV_DIR/bin/activate && pip3 install --no-cache-dir -r $APP_DIR/requirements.txt"

# Stage 2: Runtime Image
FROM python:3.12-bookworm AS runtime-image

# Set environment variables for paths
ENV APP_DIR=/app \
    VENV_DIR=/app/.venv \
    PATH="$VENV_DIR/bin:$PATH"

# Update and clean up in a single step to minimize image size
RUN apt-get update && apt-get -y upgrade \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user for security
RUN useradd --create-home --shell /bin/sh --uid 8000 opencost

# Copy application files from the builder stage
COPY --from=builder $APP_DIR $APP_DIR

# Copy all files from /src to /app
COPY src/ $APP_DIR/

# Set correct permissions for the application files
RUN chmod 755 $APP_DIR/opencost_parquet_exporter.py \
    && chown -R opencost $APP_DIR

# Switch to the non-root user
USER opencost

# Default entrypoint and command
ENTRYPOINT ["/app/.venv/bin/python3"]
CMD ["/app/opencost_parquet_exporter.py"]
