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

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
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

print_info "Stopping SIMPREDE Airflow containers..."

# Stop all containers
print_info "Stopping containers..."
$DOCKER_COMPOSE_CMD stop

# Remove containers and networks
print_info "Removing containers and networks..."
$DOCKER_COMPOSE_CMD down --remove-orphans

# Optional: Remove volumes (uncomment if you want to clean all data)
# print_warning "Removing volumes (this will delete all data)..."
# $DOCKER_COMPOSE_CMD down --volumes

print_success "All SIMPREDE Airflow containers stopped successfully"

print_info "To completely clean up including volumes, run:"
print_info "$DOCKER_COMPOSE_CMD down --volumes"
