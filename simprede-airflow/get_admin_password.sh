#!/bin/bash

# SIMPREDE Airflow Password Extractor
# This script extracts the current admin password from the running Airflow container

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

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
echo -e "${BLUE}           AIRFLOW ADMIN CREDENTIALS${NC}"
echo -e "${BLUE}================================================${NC}"

# Check .env file status first
echo -e "${BLUE}[INFO]${NC} Verifying .env file status..."
if [ -f ".env" ]; then
    if grep -q "^DB_HOST=" .env && grep -q "^DB_USER=" .env && grep -q "^DB_PASSWORD=" .env; then
        echo -e "${GREEN}✅ Database credentials are set${NC}"
    else
        echo -e "${YELLOW}⚠️ WARNING: Database credentials may be missing${NC}"
    fi
else
    echo -e "${YELLOW}⚠️ WARNING: .env file not found${NC}"
fi

ADMIN_USERNAME="admin"

# Extract password from logs
echo -e "${BLUE}[INFO]${NC} Extracting password from container logs..."
ADMIN_PASSWORD=$($DOCKER_COMPOSE_CMD logs airflow-standalone | grep -i "Password for user 'admin'" | tail -1 | sed -n "s/.*Password for user 'admin': \([^ ]*\).*/\1/p")

# If not found in logs, try the generated file
if [ -z "$ADMIN_PASSWORD" ]; then
    echo -e "${BLUE}[INFO]${NC} Checking generated password file..."
    ADMIN_PASSWORD=$($DOCKER_COMPOSE_CMD exec -T airflow-standalone cat /opt/airflow/simple_auth_manager_passwords.json.generated 2>/dev/null | grep -o '"admin": "[^"]*"' | cut -d'"' -f4 || echo "")
fi

if [ -z "$ADMIN_PASSWORD" ]; then
    echo -e "${YELLOW}Could not extract password automatically${NC}"
    echo -e "${YELLOW}Please check logs manually:${NC}"
    echo -e "${BLUE}docker compose logs airflow-standalone | grep -i password${NC}"
else
    echo -e "${GREEN}Username:${NC} $ADMIN_USERNAME"
    echo -e "${GREEN}Password:${NC} $ADMIN_PASSWORD"
    echo -e "${GREEN}Web UI:${NC} http://localhost:8080"
fi

echo -e "${BLUE}================================================${NC}"
