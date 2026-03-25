FROM python:3.11-slim

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    fuse3 \
    ca-certificates \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Install rclone
RUN curl -fsSL https://rclone.org/install.sh | bash

# Allow non-root FUSE mounts
RUN echo "user_allow_other" >> /etc/fuse.conf

# Install uv for fast dependency management
RUN pip install --no-cache-dir uv

WORKDIR /app

# Copy everything needed for install (pyproject.toml + source)
COPY pyproject.toml ./
COPY debridbridge/ debridbridge/

# Install Python dependencies + the package itself
RUN uv pip install --system --no-cache .

# Create data directory
RUN mkdir -p /data

EXPOSE 8080 8181

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

ENTRYPOINT ["python", "-m", "debridbridge.main"]
