#!/bin/bash

# Script to clear Airflow DAG run metadata and force re-parse
# This resolves the "Task Instance with dag_id X was not found" error

DAG_ID="daily_eventos_processing_optimized"
RUN_ID="scheduled__2025-11-16T06:00:00 00:00"

echo "🔧 Clearing Airflow DAG run..."
echo "DAG ID: $DAG_ID"
echo "Run ID: $RUN_ID"
echo ""

# Option 1: Using docker compose exec (if running in Docker)
if command -v docker-compose &> /dev/null; then
    echo "📦 Detected docker-compose, attempting to clear via Airflow CLI..."
    
    # Clear the specific run
    docker-compose exec -T airflow-standalone \
        airflow dags delete --yes $DAG_ID 2>/dev/null || true
    
    echo "✅ DAG deleted from metadata"
    echo "⏳ Triggering new DAG run..."
    
    # Trigger a new run
    docker-compose exec -T airflow-standalone \
        airflow dags trigger $DAG_ID 2>/dev/null
    
    echo "✅ New DAG run triggered"
else
    echo "ℹ️  docker-compose not found. If running Airflow in Docker, use:"
    echo ""
    echo "   docker-compose exec airflow-standalone airflow dags delete --yes $DAG_ID"
    echo "   docker-compose exec airflow-standalone airflow dags trigger $DAG_ID"
    echo ""
    echo "ℹ️  Or use the Airflow Web UI:"
    echo "   1. Navigate to DAGs → $DAG_ID"
    echo "   2. Click the trash icon to delete the DAG"
    echo "   3. Trigger a new run"
fi

echo ""
echo "✅ Complete! The DAG will be re-parsed with the new task."
