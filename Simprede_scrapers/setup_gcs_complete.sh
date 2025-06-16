#!/bin/bash
# SIMPREDE Airflow GCS Complete Setup Script
# This script automates the entire setup process for GCS integration

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "🚀 SIMPREDE Airflow GCS Complete Setup"
echo "======================================"

# Function to log with colors
log() {
    echo -e "\033[1;34m[INFO]\033[0m $1"
}

warn() {
    echo -e "\033[1;33m[WARN]\033[0m $1"
}

error() {
    echo -e "\033[1;31m[ERROR]\033[0m $1"
}

success() {
    echo -e "\033[1;32m[SUCCESS]\033[0m $1"
}

# Check if Docker is running
if ! docker info >/dev/null 2>&1; then
    error "Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check for project root .env file first
if [ -f "$PROJECT_ROOT/.env" ]; then
    log "Found .env file in project root: $PROJECT_ROOT/.env"
    # Copy to project directory
    cp "$PROJECT_ROOT/.env" "$PROJECT_ROOT/.env"
    success "Using .env file from project root"
elif [ ! -f "$PROJECT_ROOT/.env" ]; then
    if [ -f "$PROJECT_ROOT/.env.template" ]; then
        log "Creating .env file from template..."
        cp "$PROJECT_ROOT/.env.template" "$PROJECT_ROOT/.env"
        success "Created .env file. You can edit it to customize your configuration."
    else
        error "No .env file or .env.template found in project root"
        exit 1
    fi
else
    log ".env file already exists in project root. Skipping creation."
fi

# Create config directory
mkdir -p "$PROJECT_ROOT/config"

# Check for GCS credentials
GCS_CREDS_FILE="$PROJECT_ROOT/config/gcs-credentials.json"
if [ ! -f "$GCS_CREDS_FILE" ]; then
    warn "GCS credentials file not found: $GCS_CREDS_FILE"
    echo ""
    echo "📋 To set up GCS credentials:"
    echo "   1. Go to Google Cloud Console (https://console.cloud.google.com/)"
    echo "   2. Select or create a project"
    echo "   3. Go to IAM & Admin > Service Accounts"
    echo "   4. Create a new service account with these roles:"
    echo "      - Storage Admin (for bucket management)"
    echo "      - Storage Object Admin (for file uploads)"
    echo "   5. Create and download a JSON key"
    echo "   6. Save it as: $GCS_CREDS_FILE"
    echo ""
    
    # Create a template credentials file
    cat > "$PROJECT_ROOT/config/gcs-credentials-template.json" << 'EOF'
{
  "type": "service_account",
  "project_id": "your-project-id",
  "private_key_id": "your-private-key-id",
  "private_key": "-----BEGIN PRIVATE KEY-----\nYOUR_PRIVATE_KEY_HERE\n-----END PRIVATE KEY-----\n",
  "client_email": "your-service-account@your-project.iam.gserviceaccount.com",
  "client_id": "your-client-id",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://token.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/your-service-account%40your-project.iam.gserviceaccount.com"
}
EOF
    log "Created credentials template at: $PROJECT_ROOT/config/gcs-credentials-template.json"
    
    # Ask if user wants to continue without credentials
    echo ""
    read -p "Continue setup without GCS credentials? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Please add your GCS credentials and run this script again."
        exit 1
    fi
    warn "Continuing without GCS credentials. GCS export will not work until credentials are provided."
else
    success "GCS credentials file found: $GCS_CREDS_FILE"
    
    # Validate credentials file
    if python3 -c "import json; json.load(open('$GCS_CREDS_FILE'))" 2>/dev/null; then
        success "GCS credentials file is valid JSON"
    else
        error "GCS credentials file is not valid JSON"
        exit 1
    fi
fi

# Create data directories
log "Creating data directories..."
mkdir -p "$PROJECT_ROOT/data"/{raw,structured,processed}
mkdir -p "$PROJECT_ROOT/logs"
success "Data directories created"

# Build Docker image
log "Building Docker image with GCS support..."
if docker-compose build --no-cache; then
    success "Docker image built successfully"
else
    error "Failed to build Docker image"
    exit 1
fi

# Start the services
log "Starting Airflow services..."
if docker-compose up -d; then
    success "Airflow services started successfully"
else
    error "Failed to start Airflow services"
    exit 1
fi

# Wait for services to be ready
log "Waiting for services to be ready..."
sleep 30

# Check if Airflow is accessible
log "Checking Airflow accessibility..."
AIRFLOW_URL="http://localhost:8080"
RETRIES=12  # 2 minutes with 10 second intervals
for i in $(seq 1 $RETRIES); do
    if curl -s "$AIRFLOW_URL/health" >/dev/null 2>&1; then
        success "Airflow is accessible at $AIRFLOW_URL"
        break
    else
        if [ $i -eq $RETRIES ]; then
            error "Airflow is not accessible after waiting 2 minutes"
            docker-compose logs
            exit 1
        fi
        log "Waiting for Airflow to be ready... (attempt $i/$RETRIES)"
        sleep 10
    fi
done

# Display final information
echo ""
echo "🎉 SIMPREDE Airflow with GCS Setup Complete!"
echo "==========================================="
echo ""
echo "📊 Access Information:"
echo "   • Airflow Web UI: http://localhost:8080"
echo "   • Default credentials: admin/admin"
echo ""
echo "🗂️ Project Structure:"
echo "   • DAGs: ./dags/"
echo "   • Data: ./data/ (raw/, structured/, processed/)"
echo "   • Logs: ./logs/"
echo "   • Config: ./config/"
echo ""
echo "☁️ GCS Configuration:"
echo "   • Config file: ./scripts/google_scraper/config/gcs_config.json"
echo "   • Credentials: ./config/gcs-credentials.json"
if [ ! -f "$GCS_CREDS_FILE" ]; then
    echo "   • Status: ⚠️ Credentials not configured"
    echo "   • Action needed: Add your GCS service account JSON file"
else
    echo "   • Status: ✅ Ready to use"
fi
echo ""
echo "🚀 Next Steps:"
echo "   1. Open Airflow UI: http://localhost:8080"
echo "   2. Enable the 'pipeline_scraper_google' DAG"
echo "   3. Run the DAG to test the complete pipeline"
if [ ! -f "$GCS_CREDS_FILE" ]; then
    echo "   4. Add GCS credentials when ready: $GCS_CREDS_FILE"
    echo "   5. Restart services: docker-compose restart"
fi
echo ""
echo "📋 Useful Commands:"
echo "   • View logs: docker-compose logs -f"
echo "   • Stop services: docker-compose down"
echo "   • Restart services: docker-compose restart"
echo "   • View GCS setup: docker-compose exec airflow-standalone cat /opt/airflow/config/gcs_env.sh"
echo ""
