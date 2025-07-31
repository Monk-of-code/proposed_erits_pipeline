FROM apache/airflow:2.10.5-python3.12

# Step 1: Become root to install system-level geospatial and Java packages
USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    g++ \
    python3-dev \
    libspatialindex-dev \
    libgdal-dev \
    gdal-bin \
    libgeos-dev \
    libproj-dev \
    proj-bin\
    libgl1 \
    openjdk-17-jre-headless \
    ca-certificates \
    curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME and update PATH
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Step 2: Copy requirements
COPY requirements.txt /requirements.txt

# Step 3: Switch back to airflow user
USER airflow

# Step 4: Install Python deps
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

