#!/bin/bash
# Docker Entrypoint for SIMPREDE Airflow with automatic GCS setup
# This script ensures all necessary initialization is completed before starting Airflow

set -e

# Enable debug mode if requested
if [ "${GCS_DEBUG:-false}" = "true" ]; then
    set -x
fi

echo "🚀 SIMPREDE Airflow Container Initialization"
echo "============================================="

# Function to log with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Ensure we're running as the correct user
if [ "$(id -u)" = "0" ]; then
    log "Running as root - will switch to airflow user after setup"
    AS_ROOT=true
else
    log "Running as airflow user"
    AS_ROOT=false
fi

# Create necessary directories
log "Creating necessary directories..."
mkdir -p /opt/airflow/config
mkdir -p /opt/airflow/logs/gcs_export
mkdir -p /opt/airflow/data/{raw,structured,processed}
mkdir -p /opt/airflow/dags

# Set proper permissions if running as root
if [ "$AS_ROOT" = "true" ]; then
    chown -R airflow:root /opt/airflow/config
    chown -R airflow:root /opt/airflow/logs/gcs_export
    chown -R airflow:root /opt/airflow/data
    chown -R airflow:root /opt/airflow/dags
fi

# Source environment variables if .env file exists
# Check project root first (mounted volume), then container locations
ENV_LOCATIONS=(
    "/opt/airflow/../.env"     # Project root mounted as volume parent
    "/opt/airflow/.env"        # Container .env (copied from project root)
    "/.env"                    # Root .env
)

for env_file in "${ENV_LOCATIONS[@]}"; do
    if [ -f "$env_file" ]; then
        log "Loading environment from: $env_file"
        set -a
        source "$env_file"
        set +a
        ENV_LOADED=true
        break
    fi
done

if [ "${ENV_LOADED:-false}" != "true" ]; then
    log "⚠️ No .env file found in expected locations"
fi

# Set default GCS environment variables
export GCS_PROJECT_ID="${GCS_PROJECT_ID:-simprede}"
export GCS_BUCKET_NAME="${GCS_BUCKET_NAME:-simprede-data-pipeline}"
export GCS_LOCATION="${GCS_LOCATION:-EUROPE-WEST1}"
export GCS_CREDENTIALS_FILE="${GCS_CREDENTIALS_FILE:-/opt/airflow/config/gcs-credentials.json}"

log "GCS Configuration:"
log "  Project ID: $GCS_PROJECT_ID"
log "  Bucket Name: $GCS_BUCKET_NAME"
log "  Location: $GCS_LOCATION"
log "  Credentials: $GCS_CREDENTIALS_FILE"

# Run GCS initialization if script exists
if [ -f "/opt/airflow/scripts/init_gcs.sh" ]; then
    log "Running GCS initialization..."
    if [ "$AS_ROOT" = "true" ]; then
        su airflow -c "/opt/airflow/scripts/init_gcs.sh"
    else
        /opt/airflow/scripts/init_gcs.sh
    fi
else
    log "⚠️ GCS initialization script not found - creating basic config..."
    
    # Create basic GCS config if it doesn't exist
    GCS_CONFIG_FILE="/opt/airflow/scripts/google_scraper/config/gcs_config.json"
    if [ ! -f "$GCS_CONFIG_FILE" ]; then
        mkdir -p "$(dirname "$GCS_CONFIG_FILE")"
        cat > "$GCS_CONFIG_FILE" << EOF
{
  "project_id": "$GCS_PROJECT_ID",
  "bucket_name": "$GCS_BUCKET_NAME",
  "credentials_path": "$GCS_CREDENTIALS_FILE",
  "location": "$GCS_LOCATION",
  "description": "Configuração de Lago de dados do Simprede no Google Cloud Storage"
}
EOF
        log "✅ Created basic GCS config: $GCS_CONFIG_FILE"
    fi
fi

# Check Python dependencies
log "Verifying Python dependencies..."
python -c "
import sys
try:
    import google.cloud.storage
    print('✅ Google Cloud Storage library available')
except ImportError as e:
    print(f'⚠️ Google Cloud Storage library not available: {e}')
    
try:
    import airflow
    print(f'✅ Airflow {airflow.__version__} available')
except ImportError as e:
    print(f'❌ Airflow not available: {e}')
    sys.exit(1)
"

# Check DAG directory and files
log "Checking DAG directory..."
if [ -d "/opt/airflow/dags" ]; then
    DAG_COUNT=$(find /opt/airflow/dags -name "*.py" -type f | wc -l)
    log "Found $DAG_COUNT Python files in /opt/airflow/dags"
    if [ "$DAG_COUNT" -gt 0 ]; then
        log "DAG files found:"
        find /opt/airflow/dags -name "*.py" -type f | while read dag_file; do
            log "  - $(basename "$dag_file")"
        done
    else
        log "⚠️ No Python files found in DAGs directory"
    fi
else
    log "❌ DAGs directory does not exist"
fi

# If this is the initialization container, run database setup
if [ "${_AIRFLOW_DB_MIGRATE:-}" = "true" ]; then
    log "Running Airflow database migration..."
    airflow db migrate
    log "✅ Database migration completed"
fi

# Export GCS environment variables for the session
cat > /opt/airflow/config/gcs_env.sh << EOF
#!/bin/bash
# GCS Environment Variables - automatically generated
export GCS_PROJECT_ID="$GCS_PROJECT_ID"
export GCS_BUCKET_NAME="$GCS_BUCKET_NAME"
export GCS_LOCATION="$GCS_LOCATION"
export GOOGLE_APPLICATION_CREDENTIALS="$GCS_CREDENTIALS_FILE"
EOF

chmod +x /opt/airflow/config/gcs_env.sh

log "✅ SIMPREDE Airflow initialization completed"
log "============================================="

# Execute the original command
# If the first argument is an Airflow command, prefix it with 'airflow'
if [ "$1" = "standalone" ] || [ "$1" = "scheduler" ] || [ "$1" = "webserver" ] || [ "$1" = "worker" ] || [ "$1" = "triggerer" ]; then
    exec airflow "$@"
else
    exec "$@"
fi