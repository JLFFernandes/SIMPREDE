#!/bin/bash
# SIMPREDE Airflow GCS Demo Script
# This script demonstrates the complete automated setup and validation

set -e

echo "🎬 SIMPREDE Airflow GCS Integration Demo"
echo "======================================="

# Change to the project directory
PROJECT_DIR="/Users/ruicarvalho/Desktop/projects/SIMPREDE/simprede-airflow"
cd "$PROJECT_DIR"

echo ""
echo "📁 Current directory: $(pwd)"
echo ""

# Step 1: Run the complete setup
echo "🚀 Step 1: Running complete automated setup..."
echo "----------------------------------------------"
./setup_gcs_complete.sh

echo ""
echo "✅ Setup completed!"
echo ""

# Step 2: Validate the setup
echo "🔍 Step 2: Validating the setup..."
echo "----------------------------------"
./validate_setup.sh

echo ""
echo "✅ Validation completed!"
echo ""

# Step 3: Show how to access the system
echo "🎯 Step 3: Access Information"
echo "-----------------------------"
echo "Airflow Web UI: http://localhost:8080"
echo "Default Login: admin/admin"
echo ""

# Step 4: Show next steps
echo "📋 Step 4: Next Steps"
echo "---------------------"
echo "1. Open your browser to: http://localhost:8080"
echo "2. Login with admin/admin"
echo "3. Find the 'pipeline_scraper_google' DAG"
echo "4. Enable it by clicking the toggle switch"
echo "5. Click 'Trigger DAG' to run the complete pipeline"
echo "6. Monitor the progress in the UI"
echo ""

# Step 5: Show useful commands
echo "🛠️ Step 5: Useful Commands"
echo "-------------------------"
echo "View logs:           docker-compose logs -f"
echo "Stop services:       docker-compose down"
echo "Restart services:    docker-compose restart"
echo "Validate setup:      ./validate_setup.sh"
echo "Check GCS config:    docker-compose exec airflow-standalone cat /opt/airflow/config/gcs_env.sh"
echo ""

# Show GCS status
if [ -f "./config/gcs-credentials.json" ]; then
    echo "☁️ GCS Status: ✅ READY - Credentials configured"
    echo "   The pipeline will export data to Google Cloud Storage"
else
    echo "☁️ GCS Status: ⚠️ CREDENTIALS NEEDED"
    echo "   Add your service account JSON to: ./config/gcs-credentials.json"
    echo "   Then restart: docker-compose restart"
fi

echo ""
echo "🎉 Demo complete! Your SIMPREDE Airflow with GCS integration is ready!"
echo ""
