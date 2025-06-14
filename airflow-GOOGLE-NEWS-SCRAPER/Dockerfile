FROM apache/airflow:3.0.1-python3.11

# Switch to root to install system dependencies
USER root

# Install system dependencies for Chrome and development
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    curl \
    unzip \
    wget \
    gnupg \
    libpq-dev \
    xvfb \
    fonts-liberation \
    libnss3-dev \
    libgconf-2-4 \
    libxss1 \
    libappindicator1 \
    libasound2-dev \
    libxml2-dev \
    libxslt1-dev \
    zlib1g-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


# Instalar Chromium (compatível com ARM64 e funciona com Selenium)
RUN apt-get update && apt-get install -y chromium chromium-driver

# Copy scripts
COPY scripts/init_gcs.sh /opt/airflow/scripts/init_gcs.sh
COPY scripts/docker-entrypoint.sh /opt/airflow/scripts/docker-entrypoint.sh

# Set permissions and user
RUN chmod +x /opt/airflow/scripts/*.sh \
    && usermod -u 501 airflow \
    && chown -R airflow:root /home/airflow /opt/airflow

# Switch back to airflow user
USER airflow

# Copy and install requirements
COPY requirements.txt /opt/airflow/requirements.txt

# Install dependencies with better conflict resolution
RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r /opt/airflow/requirements.txt \
    && python -c "import airflow; print('✅ Airflow version:', airflow.__version__)" \
    && python -c "import selenium; print('✅ Selenium version:', selenium.__version__)" \
    && python -c "from airflow.providers.postgres.hooks.postgres import PostgresHook; print('✅ PostgresHook available')" \
    && python -c "from airflow.providers.mysql.hooks.mysql import MySqlHook; print('✅ MySqlHook available')"

# Set environment variables
ENV PYTHONPATH="/opt/airflow"
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False
