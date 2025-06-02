FROM apache/airflow:2.8.1-python3.11

# Switch to root to install system packages
USER root

# Install system dependencies
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
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libglib2.0-0 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libexpat1 \
    libfontconfig1 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Chrome/Chromium and related dependencies
RUN apt-get update && \
    apt-get install -y wget gnupg && \
    ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then \
        # x86_64 architecture - Use Google Chrome official repo
        wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - && \
        echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list && \
        apt-get update && \
        apt-get install -y google-chrome-stable; \
    else \
        # ARM64 architecture - Use Chromium instead
        apt-get install -y chromium chromium-driver; \
    fi && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create symlinks for compatibility and ensure proper directory permissions
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then \
        # x86_64 architecture - Use Google Chrome
        ln -sf /usr/bin/google-chrome /usr/bin/chromium && \
        ln -sf /usr/bin/google-chrome /usr/bin/chromium-browser && \
        # Set Chrome environment variables
        echo "export CHROME_BIN=/usr/bin/google-chrome" >> /etc/environment && \
        echo "export CHROMEDRIVER_PATH=/usr/bin/chromedriver" >> /etc/environment; \
    else \
        # ARM64 architecture - Use Chromium
        ln -sf /usr/bin/chromium /usr/bin/google-chrome && \
        ln -sf /usr/bin/chromium /usr/bin/google-chrome-stable && \
        # Set Chromium environment variables
        echo "export CHROME_BIN=/usr/bin/chromium" >> /etc/environment && \
        echo "export CHROMEDRIVER_PATH=/usr/bin/chromedriver" >> /etc/environment; \
    fi && \
    mkdir -p /tmp/.X11-unix && \
    chmod 1777 /tmp/.X11-unix && \
    # Make sure the binaries are in the PATH for all users
    echo "export PATH=$PATH:/usr/bin" >> /home/airflow/.bashrc && \
    echo "export PATH=$PATH:/usr/bin" >> /root/.bashrc

# Set environment variables for Chrome/Chromium based on architecture
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then \
        echo "export CHROME_BIN=/usr/bin/google-chrome" > /etc/profile.d/chrome.sh; \
    else \
        echo "export CHROME_BIN=/usr/bin/chromium" > /etc/profile.d/chrome.sh; \
    fi && \
    echo "export CHROMEDRIVER_PATH=/usr/bin/chromedriver" >> /etc/profile.d/chrome.sh && \
    echo "export PATH=/usr/bin:\$PATH" >> /etc/profile.d/chrome.sh && \
    chmod +x /etc/profile.d/chrome.sh

# Set environment variables in ENV as well
ENV PATH="/usr/bin:${PATH}"
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver

# Install ChromeDriver (make sure it matches Chrome version)
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then \
        # x86_64 architecture - Use ChromeDriver from Google
        if [ -f "/usr/bin/google-chrome" ]; then \
            CHROME_VERSION=$(google-chrome --version | awk '{print $3}' | cut -d. -f1) && \
            CHROMEDRIVER_VERSION=$(curl -s "https://chromedriver.storage.googleapis.com/LATEST_RELEASE_${CHROME_VERSION}") && \
            wget -q "https://chromedriver.storage.googleapis.com/${CHROMEDRIVER_VERSION}/chromedriver_linux64.zip" -O /tmp/chromedriver.zip && \
            unzip /tmp/chromedriver.zip -d /tmp/ && \
            mv /tmp/chromedriver /usr/bin/chromedriver && \
            chmod +x /usr/bin/chromedriver && \
            rm /tmp/chromedriver.zip; \
        fi; \
    else \
        # ARM64 architecture - ChromeDriver should already be installed via Chromium package
        # Make sure it's executable
        if [ -f "/usr/bin/chromedriver" ]; then \
            chmod +x /usr/bin/chromedriver; \
        fi; \
    fi && \
    # Verify installation
    ls -la /usr/bin/chromium /usr/bin/chromedriver 2>/dev/null || true && \
    chromium --version || true && \
    chromedriver --version || true

# Switch back to airflow user
USER airflow

# Copy requirements and install them during build
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Verify installations
RUN python -c "import sys; print(f'Python version: {sys.version}')"
RUN python -c "import selenium; print(f'Selenium version: {selenium.__version__}')"

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV SELENIUM_HEADLESS=1
ENV DISPLAY=:99
ENV PYTHONDONTWRITEBYTECODE=1

# Define entry point and command from the base image
