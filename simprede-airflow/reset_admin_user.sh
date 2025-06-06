#!/bin/bash

# SIMPREDE Airflow Admin User Reset Script
# This script resets the admin user to ensure fixed credentials

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Change to script directory
cd "$(dirname "$0")"

# Check for docker-compose command
if command -v docker-compose > /dev/null 2>&1; then
    DOCKER_COMPOSE_CMD="docker-compose"
elif docker compose version > /dev/null 2>&1; then
    DOCKER_COMPOSE_CMD="docker compose"
else
    echo -e "${RED}[ERROR]${NC} Neither 'docker-compose' nor 'docker compose' is available."
    exit 1
fi

echo -e "${BLUE}================================================${NC}"
print_info "RESETTING AIRFLOW ADMIN USER"
echo -e "${BLUE}================================================${NC}"

print_warning "This will reset the admin user (new random password will be generated)"
echo ""
read -p "Do you want to continue? (y/N): " -r
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    print_info "Operation cancelled"
    exit 0
fi

# Stop containers
print_info "Stopping Airflow containers..."
$DOCKER_COMPOSE_CMD down

# Remove volumes to clear any existing user data
print_info "Removing PostgreSQL volume to reset database..."
docker volume rm simprede-airflow_postgres-db-volume 2>/dev/null || true

# Start containers fresh
print_info "Starting containers with fresh database..."
$DOCKER_COMPOSE_CMD up -d

print_info "Waiting for initialization to complete..."
sleep 30

# Extract the actual admin password
print_info "Extracting admin credentials..."

ADMIN_USERNAME="admin"

# Extract password from logs
ADMIN_PASSWORD=$($DOCKER_COMPOSE_CMD logs airflow-standalone | grep -i "Password for user 'admin'" | tail -1 | sed -n "s/.*Password for user 'admin': \([^ ]*\).*/\1/p")

# If not found in logs, try the generated file
if [ -z "$ADMIN_PASSWORD" ]; then
    ADMIN_PASSWORD=$($DOCKER_COMPOSE_CMD exec -T airflow-standalone cat /opt/airflow/simple_auth_manager_passwords.json.generated 2>/dev/null | grep -o '"admin": "[^"]*"' | cut -d'"' -f4 || echo "")
fi

print_success "Admin user reset complete!"

if [ -z "$ADMIN_PASSWORD" ]; then
    echo -e "${YELLOW}Password:${NC} <check logs with: docker compose logs airflow-standalone | grep password>"
else
    echo -e "${GREEN}Username:${NC} $ADMIN_USERNAME"
    echo -e "${GREEN}Password:${NC} $ADMIN_PASSWORD"
fi

echo -e "${GREEN}Web UI:${NC} http://localhost:8080"

echo ""
print_info "Container status:"
$DOCKER_COMPOSE_CMD ps
