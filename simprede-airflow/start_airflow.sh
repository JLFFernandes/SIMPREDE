#!/bin/bash

# SIMPREDE Airflow Startup Script
# This script builds and starts the Airflow Docker containers and displays the admin credentials

set -e  # Exit on any error

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

# Function to check if docker-compose is available
check_docker_compose() {
    if command -v docker-compose > /dev/null 2>&1; then
        DOCKER_COMPOSE_CMD="docker-compose"
    elif docker compose version > /dev/null 2>&1; then
        DOCKER_COMPOSE_CMD="docker compose"
    else
        print_error "Neither 'docker-compose' nor 'docker compose' is available. Please install Docker Compose."
        exit 1
    fi
    print_success "Found Docker Compose: $DOCKER_COMPOSE_CMD"
}

# Function to set proper permissions
set_permissions() {
    print_info "Setting up directory permissions..."
    
    # Create directories if they don't exist
    mkdir -p logs dags plugins data config scripts
    
    # Set AIRFLOW_UID environment variable
    export AIRFLOW_UID=$(id -u)
    echo "AIRFLOW_UID=$AIRFLOW_UID" > .env
    
    print_success "Permissions set (AIRFLOW_UID=$AIRFLOW_UID)"
}

# Function to create necessary directories
create_directories() {
    print_info "Creating necessary directories..."
    
    # Create data directories on host
    mkdir -p ./data/raw
    mkdir -p ./data/structured  
    mkdir -p ./data/processed
    mkdir -p ./logs
    mkdir -p ./plugins
    mkdir -p ./config
    
    # Set proper permissions
    chmod 755 ./data ./logs ./plugins ./config
    chmod -R 755 ./data/raw ./data/structured ./data/processed
    
    print_success "Directories created successfully"
}

# Function to build and start containers
start_containers() {
    print_info "Building and starting Airflow containers..."
    
    # Set AIRFLOW_UID if not already set
    if [ -z "${AIRFLOW_UID}" ]; then
        export AIRFLOW_UID=$(id -u)
        echo "AIRFLOW_UID=${AIRFLOW_UID}" >> .env
        print_info "Set AIRFLOW_UID to ${AIRFLOW_UID}"
    fi
    
    # Build and start containers
    if command -v docker-compose > /dev/null 2>&1; then
        docker-compose up --build -d
    elif docker compose version > /dev/null 2>&1; then
        docker compose up --build -d
    else
        print_error "Neither 'docker-compose' nor 'docker compose' is available."
        exit 1
    fi
    
    print_success "Containers started successfully"
}

# Function to wait for services to be ready
wait_for_services() {
    print_info "Waiting for services to be ready..."
    
    # Wait for PostgreSQL
    print_info "Waiting for PostgreSQL to be ready..."
    timeout=60
    counter=0
    
    while [ $counter -lt $timeout ]; do
        if $DOCKER_COMPOSE_CMD exec -T postgres pg_isready -U airflow > /dev/null 2>&1; then
            print_success "PostgreSQL is ready"
            break
        fi
        
        if [ $counter -eq 0 ]; then
            echo -n "Waiting"
        fi
        echo -n "."
        sleep 2
        counter=$((counter + 2))
    done
    
    if [ $counter -ge $timeout ]; then
        print_error "PostgreSQL failed to start within $timeout seconds"
        return 1
    fi
    
    echo ""  # New line after dots
    
    # Wait for Airflow webserver
    print_info "Waiting for Airflow webserver to be ready..."
    timeout=60
    counter=0
    
    while [ $counter -lt $timeout ]; do
        if curl -f http://localhost:8080/api/v2/monitor/health > /dev/null 2>&1; then
            print_success "Airflow webserver is ready"
            break
        fi
        
        if [ $counter -eq 0 ]; then
            echo -n "Waiting"
        fi
        echo -n "."
        sleep 3
        counter=$((counter + 3))
    done
    
    if [ $counter -ge $timeout ]; then
        print_warning "Airflow webserver health check timeout, but it might still be starting..."
    fi
    
    echo ""  # New line after dots
}

# Function to display admin credentials
display_credentials() {
    print_separator
    print_info "AIRFLOW ADMIN CREDENTIALS"
    print_separator
    
    ADMIN_USERNAME="admin"
    
    # Extract the actual password from Airflow logs
    print_info "Extracting admin password from Airflow container..."
    
    # First try to get password from logs
    ADMIN_PASSWORD=$($DOCKER_COMPOSE_CMD logs airflow-standalone | grep -i "Password for user 'admin'" | tail -1 | sed -n "s/.*Password for user 'admin': \([^ ]*\).*/\1/p")
    
    # If not found in logs, try to get from the generated file
    if [ -z "$ADMIN_PASSWORD" ]; then
        print_info "Password not found in logs, checking generated file..."
        ADMIN_PASSWORD=$($DOCKER_COMPOSE_CMD exec -T airflow-standalone cat /opt/airflow/simple_auth_manager_passwords.json.generated 2>/dev/null | grep -o '"admin": "[^"]*"' | cut -d'"' -f4 || echo "")
    fi
    
    if [ -z "$ADMIN_PASSWORD" ]; then
        print_warning "Could not extract password automatically"
        echo -e "${YELLOW}Please check logs manually:${NC}"
        echo -e "${BLUE}docker compose logs airflow-standalone | grep -i password${NC}"
        ADMIN_PASSWORD="<check logs>"
    fi
    
    echo -e "${GREEN}Username:${NC} $ADMIN_USERNAME"
    echo -e "${GREEN}Password:${NC} $ADMIN_PASSWORD"
    echo -e "${GREEN}Web UI:${NC} http://localhost:8080"
    
    print_separator
    print_info "Password is auto-generated by Airflow Simple Auth Manager"
}

# Function to display container status
display_status() {
    print_separator
    print_info "CONTAINER STATUS"
    print_separator
    $DOCKER_COMPOSE_CMD ps
}

# Function to display useful commands
display_commands() {
    print_separator
    print_info "USEFUL COMMANDS"
    print_separator
    echo -e "${YELLOW}View logs:${NC}"
    echo "  $DOCKER_COMPOSE_CMD logs airflow-standalone"
    echo "  $DOCKER_COMPOSE_CMD logs postgres"
    echo ""
    echo -e "${YELLOW}Stop services:${NC}"
    echo "  $DOCKER_COMPOSE_CMD down"
    echo ""
    echo -e "${YELLOW}Restart services:${NC}"
    echo "  $DOCKER_COMPOSE_CMD restart"
    echo ""
    echo -e "${YELLOW}Access container shell:${NC}"
    echo "  $DOCKER_COMPOSE_CMD exec airflow-standalone bash"
    echo ""
    echo -e "${YELLOW}Force rebuild:${NC}"
    echo "  $DOCKER_COMPOSE_CMD down && $DOCKER_COMPOSE_CMD build --no-cache && $DOCKER_COMPOSE_CMD up -d"
}

# Main execution
main() {
    print_separator
    print_info "SIMPREDE AIRFLOW STARTUP SCRIPT"
    print_separator
    
    # Change to script directory
    cd "$(dirname "$0")"
    print_info "Working directory: $(pwd)"
    
    # Pre-flight checks
    check_docker
    check_docker_compose
    
    # Setup
    set_permissions
    create_directories
    
    # Ask user if they want to continue
    echo ""
    read -p "Do you want to build and start Airflow? (y/N): " -r
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_info "Operation cancelled by user"
        exit 0
    fi
    
    # Stop any existing containers
    print_info "Stopping any existing containers..."
    $DOCKER_COMPOSE_CMD down > /dev/null 2>&1 || true
    
    # Build and start
    start_containers
    
    # Wait for services
    wait_for_services
    
    # Display information
    display_credentials
    display_status
    display_commands
    
    print_separator
    print_success "Airflow is now running! Access the web UI at http://localhost:8080"
    print_separator
}

# Handle script interruption
trap 'print_error "Script interrupted. You may need to run: $DOCKER_COMPOSE_CMD down"; exit 1' INT TERM

# Run main function
main "$@"
