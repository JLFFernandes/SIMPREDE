#!/bin/bash

# SIMPREDE Airflow Stop Script
# This script stops all Airflow Docker containers

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

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

print_info "Stopping Airflow containers..."
$DOCKER_COMPOSE_CMD down

print_success "Airflow containers stopped successfully!"

# Ask if user wants to remove volumes (data)
echo ""
read -p "Do you want to remove all data volumes? This will delete all data! (y/N): " -r
if [[ $REPLY =~ ^[Yy]$ ]]; then
    print_warning "Removing volumes..."
    $DOCKER_COMPOSE_CMD down -v
    print_warning "All data has been removed!"
else
    print_info "Data volumes preserved"
fi
