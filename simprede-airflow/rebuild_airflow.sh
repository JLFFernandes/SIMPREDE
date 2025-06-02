#!/bin/bash
# Script to rebuild and restart Airflow containers

echo "Stopping and removing existing containers..."
docker-compose down

echo "Cleaning up Docker cache..."
docker system prune -f

echo "Building the Airflow image..."
docker-compose build --no-cache

echo "Starting Airflow containers..."
docker-compose up -d

echo "Checking container status..."
docker-compose ps

echo "Waiting for services to initialize (30 seconds)..."
sleep 30

echo "Verifying Chrome installation in the container..."
docker-compose exec airflow-scheduler bash /opt/airflow/verify_chrome.sh

echo "Checking Airflow logs..."
docker-compose logs airflow-init

echo "Done! You can access the Airflow UI at http://localhost:8080"
echo "Username: admin"
echo "Password: simprede"
