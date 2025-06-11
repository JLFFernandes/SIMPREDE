#!/bin/bash
# SIMPREDE Airflow GCS Validation Script
# This script validates that the complete GCS integration is working properly

set -e

echo "🔍 SIMPREDE Airflow GCS Setup Validation"
echo "========================================"

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

# Check Docker status
log "Checking Docker services..."
if docker-compose ps | grep -q "Up"; then
    success "Docker services are running"
else
    error "Docker services are not running. Run: docker-compose up -d"
    exit 1
fi

# Check Airflow accessibility
log "Checking Airflow accessibility..."
if curl -s "http://localhost:8080/health" >/dev/null 2>&1; then
    success "Airflow is accessible at http://localhost:8080"
else
    error "Airflow is not accessible. Check service status."
    exit 1
fi

# Check configuration files
log "Checking configuration files..."

# Check .env file
if [ -f ".env" ]; then
    success ".env file exists"
else
    warn ".env file not found - using defaults"
fi

# Check GCS config
GCS_CONFIG="/scripts/google_scraper/config/gcs_config.json"
if docker-compose exec -T airflow-standalone test -f "$GCS_CONFIG"; then
    success "GCS config file exists"
    # Validate JSON
    if docker-compose exec -T airflow-standalone python3 -c "import json; json.load(open('$GCS_CONFIG'))" 2>/dev/null; then
        success "GCS config is valid JSON"
    else
        error "GCS config is not valid JSON"
    fi
else
    error "GCS config file not found: $GCS_CONFIG"
fi

# Check GCS credentials
GCS_CREDS="/opt/airflow/config/gcs-credentials.json"
if docker-compose exec -T airflow-standalone test -f "$GCS_CREDS"; then
    success "GCS credentials file exists"
    # Validate JSON
    if docker-compose exec -T airflow-standalone python3 -c "import json; json.load(open('$GCS_CREDS'))" 2>/dev/null; then
        success "GCS credentials file is valid JSON"
    else
        error "GCS credentials file is not valid JSON"
    fi
else
    warn "GCS credentials file not found: $GCS_CREDS"
    echo "   Add your service account JSON file to: ./config/gcs-credentials.json"
fi

# Check Python dependencies
log "Checking Python dependencies..."
if docker-compose exec -T airflow-standalone python3 -c "import google.cloud.storage; print('GCS client library available')" 2>/dev/null; then
    success "Google Cloud Storage library is available"
else
    error "Google Cloud Storage library is not available"
fi

# Check environment variables
log "Checking environment variables..."
GCS_PROJECT=$(docker-compose exec -T airflow-standalone bash -c 'echo $GCS_PROJECT_ID')
GCS_BUCKET=$(docker-compose exec -T airflow-standalone bash -c 'echo $GCS_BUCKET_NAME')

if [ -n "$GCS_PROJECT" ] && [ "$GCS_PROJECT" != "null" ]; then
    success "GCS_PROJECT_ID is set: $GCS_PROJECT"
else
    error "GCS_PROJECT_ID is not set"
fi

if [ -n "$GCS_BUCKET" ] && [ "$GCS_BUCKET" != "null" ]; then
    success "GCS_BUCKET_NAME is set: $GCS_BUCKET"
else
    error "GCS_BUCKET_NAME is not set"
fi

# Check data directories
log "Checking data directories..."
for dir in raw structured processed; do
    if docker-compose exec -T airflow-standalone test -d "/opt/airflow/data/$dir"; then
        success "Data directory exists: $dir"
    else
        error "Data directory missing: $dir"
    fi
done

# Check DAG availability
log "Checking DAG availability..."
if docker-compose exec -T airflow-standalone airflow dags list | grep -q "pipeline_scraper_google"; then
    success "Pipeline DAG is available"
else
    error "Pipeline DAG not found"
fi

# Test GCS export module
log "Testing GCS export module..."
if docker-compose exec -T airflow-standalone python3 -c "
import sys
sys.path.insert(0, '/opt/airflow/scripts/google_scraper/exportador_gcs')
try:
    from export_to_gcs_airflow import get_gcs_config
    config = get_gcs_config()
    print(f'GCS module loaded successfully')
    print(f'Project: {config[\"project_id\"]}')
    print(f'Bucket: {config[\"bucket_name\"]}')
except Exception as e:
    print(f'Error: {e}')
    exit(1)
" 2>/dev/null; then
    success "GCS export module is working"
else
    warn "GCS export module test failed (may need credentials)"
fi

echo ""
echo "🎯 Validation Summary"
echo "===================="

# Count successes and errors
SUCCESS_COUNT=$(grep -c "SUCCESS" /tmp/validation_log 2>/dev/null || echo "0")
ERROR_COUNT=$(grep -c "ERROR" /tmp/validation_log 2>/dev/null || echo "0")

if [ -f "./config/gcs-credentials.json" ]; then
    echo "✅ Setup Status: READY"
    echo "   You can run the pipeline and export to GCS"
else
    echo "⚠️  Setup Status: PARTIAL"
    echo "   Add GCS credentials to complete setup"
fi

echo ""
echo "📋 Next Steps:"
echo "   1. Open Airflow UI: http://localhost:8080"
echo "   2. Enable the 'pipeline_scraper_google' DAG"
echo "   3. Run a test execution"

if [ ! -f "./config/gcs-credentials.json" ]; then
    echo "   4. Add GCS credentials: ./config/gcs-credentials.json"
    echo "   5. Restart services: docker-compose restart"
fi

echo ""
