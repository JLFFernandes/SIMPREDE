FROM apache/airflow:3.0.1-python3.11

# Switch to root to install system dependencies
USER root

# Install comprehensive system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    curl \
    unzip \
    wget \
    gnupg \
    ca-certificates \
    libpq-dev \
    xvfb \
    fonts-liberation \
    libappindicator3-1 \
    libappindicator1 \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libgtk-3-0 \
    libnss3-dev \
    libnss3 \
    libgconf-2-4 \
    libxss1 \
    libxml2-dev \
    libxslt1-dev \
    zlib1g-dev \
    xdg-utils \
    gdal-bin \
    libgdal-dev \
    libproj-dev \
    proj-data \
    proj-bin \
    libgeos-dev \
    libspatialindex-dev \
    python3-dev \
    build-essential \
    --no-install-recommends && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Chrome/Chromium based on architecture
RUN ARCH=$(dpkg --print-architecture) && \
    if [ "$ARCH" = "amd64" ]; then \
        # Install Google Chrome for AMD64
        curl -fsSL https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-linux-keyring.gpg && \
        echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-linux-keyring.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list && \
        apt-get update && \
        apt-get install -y google-chrome-stable && \
        rm -rf /var/lib/apt/lists/*; \
    else \
        # Install Chromium for ARM64 and other architectures
        apt-get update && \
        apt-get install -y chromium chromium-driver && \
        rm -rf /var/lib/apt/lists/*; \
    fi

# Set Chrome environment variable based on architecture
RUN ARCH=$(dpkg --print-architecture) && \
    if [ "$ARCH" = "amd64" ]; then \
        echo "export CHROME_BIN=/usr/bin/google-chrome-stable" >> /etc/environment; \
    else \
        echo "export CHROME_BIN=/usr/bin/chromium" >> /etc/environment; \
    fi

ENV CHROME_BIN=/usr/bin/chromium

# Install ChromeDriver compatible with architecture
RUN ARCH=$(dpkg --print-architecture) && \
    if [ "$ARCH" = "amd64" ]; then \
        CHROMEDRIVER_URL="https://storage.googleapis.com/chrome-for-testing-public/131.0.6778.204/linux64/chromedriver-linux64.zip"; \
        wget -O /tmp/chromedriver.zip "$CHROMEDRIVER_URL" && \
        unzip /tmp/chromedriver.zip -d /opt/chromedriver && \
        mv /opt/chromedriver/chromedriver-linux64/chromedriver /usr/bin/chromedriver && \
        chmod +x /usr/bin/chromedriver && \
        rm -rf /tmp/chromedriver.zip /opt/chromedriver; \
    else \
        # For ARM64, use the system package manager installed chromium-driver
        echo "Using system-installed chromium-driver for ARM64"; \
    fi

# Create directories and set permissions
RUN mkdir -p /usr/local/airflow/scripts /usr/local/airflow/include /usr/local/airflow/logs /usr/local/airflow/dags /usr/local/airflow/plugins

# Copy scripts
COPY scripts/init_gcs.sh /usr/local/airflow/scripts/init_gcs.sh
COPY scripts/docker-entrypoint.sh /usr/local/airflow/scripts/docker-entrypoint.sh
COPY scripts /usr/local/airflow/scripts

# Copy data files
COPY include/pts_disaster_export.xlsx /usr/local/airflow/include/pts_disaster_export.xlsx
COPY include/eswd_database.csv /usr/local/airflow/include/eswd_database.csv
COPY include/centroides /usr/local/airflow/include/centroides

# Set permissions for airflow user - ensure airflow has write access to all directories
RUN chown -R airflow:root /usr/local/airflow && \
    chmod -R 755 /usr/local/airflow && \
    chmod -R 775 /usr/local/airflow/logs /usr/local/airflow/dags /usr/local/airflow/plugins

# Switch to airflow user
USER airflow

# Copy and install requirements
COPY requirements.txt /usr/local/airflow/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r /usr/local/airflow/requirements.txt && \
    python -c "import airflow; print('✅ Airflow version:', airflow.__version__)" && \
    python -c "import selenium; print('✅ Selenium version:', selenium.__version__)" && \
    python -c "from airflow.providers.postgres.hooks.postgres import PostgresHook; print('✅ PostgresHook available')" && \
    python -c "from airflow.providers.mysql.hooks.mysql import MySqlHook; print('✅ MySqlHook available')"

# Set environment variables
ENV PYTHONPATH="/usr/local/airflow"
ENV AIRFLOW_HOME="/usr/local/airflow"
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False
ENV AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8080
ENV AIRFLOW__WEBSERVER__WEB_SERVER_HOST=0.0.0.0
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
# Force PostgreSQL usage instead of SQLite
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow

# Expose the port
EXPOSE 8080

# Add a proper health check at the end
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1
