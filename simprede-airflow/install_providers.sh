#!/bin/bash

# SIMPREDE Airflow Provider Installation Script
# This script rebuilds the Airflow container with the necessary database providers

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
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

print_separator() {
    echo -e "${BLUE}================================================${NC}"
}

# Function to check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        print_error "Docker is not running. Please start Docker Desktop and try again."
        exit 1
    fi
}

# Function to check docker-compose
check_docker_compose() {
    if command -v docker-compose > /dev/null 2>&1; then
        DOCKER_COMPOSE_CMD="docker-compose"
    elif docker compose version > /dev/null 2>&1; then
        DOCKER_COMPOSE_CMD="docker compose"
    else
        print_error "Neither 'docker-compose' nor 'docker compose' is available."
        exit 1
    fi
}

# Main function
main() {
    print_separator
    print_info "SIMPREDE AIRFLOW PROVIDER INSTALLATION"
    print_separator
    
    # Change to script directory
    cd "$(dirname "$0")"
    print_info "Working directory: $(pwd)"
    
    # Check prerequisites
    check_docker
    check_docker_compose
    
    print_info "This script will:"
    print_info "1. Stop current Airflow containers"
    print_info "2. Rebuild Docker image with PostgreSQL and MySQL providers"
    print_info "3. Start the containers with the new providers"
    
    echo ""
    read -p "Do you want to continue? (y/N): " -r
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_info "Operation cancelled by user"
        exit 0
    fi
    
    # Stop containers
    print_info "Stopping Airflow containers..."
    $DOCKER_COMPOSE_CMD down
    
    # Remove old build cache
    print_info "Cleaning Docker build cache..."
    docker builder prune -f
    
    # Rebuild with no cache
    print_info "Rebuilding Airflow image with database providers..."
    $DOCKER_COMPOSE_CMD build --no-cache airflow-standalone
    
    # Start containers
    print_info "Starting Airflow containers..."
    $DOCKER_COMPOSE_CMD up -d
    
    # Wait a bit for startup
    print_info "Waiting for services to start..."
    sleep 30
    
    # Check provider installation
    print_info "Checking provider installation..."
    $DOCKER_COMPOSE_CMD exec airflow-standalone python -c "
try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.providers.postgres.operators.postgres import PostgresOperator
    print('✅ PostgreSQL provider installed successfully')
except ImportError as e:
    print(f'❌ PostgreSQL provider failed: {e}')

try:
    from airflow.providers.mysql.hooks.mysql import MySqlHook
    from airflow.providers.mysql.operators.mysql import MySqlOperator
    print('✅ MySQL provider installed successfully')
except ImportError as e:
    print(f'❌ MySQL provider failed: {e}')
"
    
    print_separator
    print_success "Provider installation complete!"
    print_info "You can now run your SQL queries DAG"
    print_info "Access Airflow at: http://localhost:8080"
    print_separator
}

# Run main function
main "$@"