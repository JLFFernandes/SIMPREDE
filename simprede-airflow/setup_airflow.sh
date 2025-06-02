#!/bin/bash
# setup_airflow.sh - Script to set up and run the Airflow container

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Set the correct permissions
echo "Setting up permissions..."
mkdir -p ./dags ./logs ./plugins ./data
chmod -R 777 ./dags ./logs ./plugins ./data

# Build and start the containers
echo "Using the rebuild_airflow.sh script to set up the environment..."
./rebuild_airflow.sh

# Check the exit code of the rebuild script
if [ $? -eq 0 ]; then
    echo "Airflow is now running!"
    echo "You can access the Airflow web interface at: http://localhost:8080"
    echo "Username: admin"
    echo "Password: simprede"
else
    echo "Failed to start Airflow containers. Please check the logs."
    exit 1
fi

echo "Setup complete."
