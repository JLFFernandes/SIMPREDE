# Chrome/Selenium Environment Setup for Airflow

This document provides information on how the Chrome and Selenium environment is set up in the Airflow container for the Google Scraper pipeline.

## Environment Setup

The Airflow container includes the following components:

1. **Google Chrome**: Installed from Google's official repository
2. **ChromeDriver**: Automatically matched to the Chrome version
3. **Required libraries**: All necessary dependencies for headless Chrome operation
4. **Environment variables**: Configuration for Selenium and Chrome

## Key Components

- Chrome binary location: `/usr/bin/google-chrome`
- ChromeDriver location: `/usr/bin/chromedriver`
- Symlinks for compatibility: 
  - `/usr/bin/chromium -> /usr/bin/google-chrome`
  - `/usr/bin/chromium-browser -> /usr/bin/google-chrome`

## Environment Variables

- `CHROME_BIN`: Points to Chrome binary location
- `CHROMEDRIVER_PATH`: Points to ChromeDriver location
- `DISPLAY`: Set to `:99` for headless operation
- `SELENIUM_HEADLESS`: Set to `1` to force headless mode
- `PATH`: Updated to include `/usr/bin` to ensure Chrome is in PATH

## Rebuilding and Restarting

If you need to rebuild and restart the environment, use the provided script:

```bash
./rebuild_airflow.sh
```

This script will:
1. Stop and remove existing containers
2. Clean Docker cache
3. Rebuild the Airflow image
4. Start the containers
5. Verify Chrome installation
6. Show relevant logs

## Verification

You can verify the Chrome installation by running:

```bash
docker-compose exec airflow-scheduler bash /opt/airflow/verify_chrome.sh
```

This script checks for:
- Chrome and ChromeDriver binaries
- Executable permissions
- Environment variables
- Ability to run Chrome and ChromeDriver

## Common Issues and Solutions

### Chrome Not Found in PATH

**Issue**: `google-chrome: executable file not found in $PATH`

**Solution**:
1. The PATH environment variable needs to include `/usr/bin`
2. In the container, run:
   ```bash
   export PATH="/usr/bin:$PATH"
   ```
3. To make this permanent, add it to the Dockerfile

### SQLAlchemy Argument Error

**Issue**: `sqlalchemy.exc.ArgumentError: Invalid value for 'executemany_mode': 'values'`

**Solution**:
1. Add the following environment variable to docker-compose.yml:
   ```
   AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_CMD_PARAMS: '{"executemany_mode": "batch"}'
   ```
2. Make sure auth_backends includes session auth:
   ```
   AIRFLOW__API__AUTH_BACKENDS: 'airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session'
   ```

### Permission Issues

**Issue**: Chrome or ChromeDriver is not executable

**Solution**:
1. Check and fix permissions:
   ```bash
   docker-compose exec airflow-scheduler chmod +x /usr/bin/google-chrome /usr/bin/chromedriver
   ```
2. Verify the symlinks:
   ```bash
   docker-compose exec airflow-scheduler ls -la /usr/bin/google-chrome /usr/bin/chromium /usr/bin/chromedriver
   ```

## Additional Debugging

If the container is still failing to initialize:

1. Check container-specific logs:
   ```bash
   docker-compose logs airflow-init
   ```

2. Access the container directly:
   ```bash
   docker-compose exec airflow-scheduler bash
   ```

3. Run Python test script:
   ```bash
   docker-compose exec airflow-scheduler python /opt/airflow/dags/google_scraper/tests/verify_chrome_installation.py
   ```

## Architecture Notes

- Airflow uses a custom Docker image defined in the project's Dockerfile
- The Chrome installation is performed during the Docker image build
- All configuration is handled through environment variables
- The Selenium setup runs in headless mode with virtual display
