#!/bin/bash

# SIMPREDE Local Development Orchestrator
# This script runs both the scrapers (Airflow) container and the dashboard locally

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROJECT_ROOT="/Users/ruicarvalho/Desktop/projects/SIMPREDE"
SCRAPERS_DIR="$PROJECT_ROOT/Simprede_scrapers"
DASHBOARD_DIR="$PROJECT_ROOT/Simprede_dashboard"
DASHBOARD_VENV="$DASHBOARD_DIR/env"
DASHBOARD_PORT=8501
AIRFLOW_PORT=8080

# Logging function
log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Function to check if a port is in use
check_port() {
    local port=$1
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
        return 0  # Port is in use
    else
        return 1  # Port is free
    fi
}

# Function to kill processes on a specific port
kill_port() {
    local port=$1
    log "Checking for processes on port $port..."
    if check_port $port; then
        warning "Port $port is in use. Killing processes..."
        lsof -ti:$port | xargs kill -9 2>/dev/null || true
        sleep 2
    fi
}

# Function to setup dashboard environment
setup_dashboard() {
    log "Setting up dashboard environment..."
    
    cd "$DASHBOARD_DIR"
    
    # Check if virtual environment exists
    if [ ! -d "$DASHBOARD_VENV" ]; then
        log "Creating virtual environment for dashboard..."
        python3 -m venv env
    fi
    
    # Activate virtual environment and install dependencies
    log "Installing dashboard dependencies..."
    source "$DASHBOARD_VENV/bin/activate"
    pip install --upgrade pip
    pip install -r requirements.txt
    
    success "Dashboard environment setup complete"
}

# Function to start the dashboard
start_dashboard() {
    log "Starting Streamlit dashboard..."
    
    cd "$DASHBOARD_DIR"
    
    # Kill any existing processes on dashboard port
    kill_port $DASHBOARD_PORT
    
    # Activate virtual environment
    source "$DASHBOARD_VENV/bin/activate"
    
    # Start Streamlit in background
    nohup streamlit run app.py --server.port=$DASHBOARD_PORT --server.address=0.0.0.0 > dashboard.log 2>&1 &
    DASHBOARD_PID=$!
    
    # Wait a moment and check if the process started successfully
    sleep 5
    if ps -p $DASHBOARD_PID > /dev/null; then
        success "Dashboard started successfully on http://localhost:$DASHBOARD_PORT (PID: $DASHBOARD_PID)"
        echo $DASHBOARD_PID > dashboard.pid
    else
        error "Failed to start dashboard. Check dashboard.log for details."
        exit 1
    fi
}

# Function to stop Docker containers thoroughly
stop_docker_containers() {
    log "Stopping Docker containers thoroughly..."
    
    cd "$SCRAPERS_DIR"
    
    # Check if Docker is accessible before trying to stop containers
    if ! docker ps >/dev/null 2>&1; then
        warning "Docker is not accessible - skipping container cleanup"
        kill_port $AIRFLOW_PORT
        return 0
    fi
    
    # Stop and remove containers with timeout
    log "Stopping containers gracefully..."
    docker-compose stop --timeout 30 2>/dev/null || true
    
    # Force remove containers and networks
    log "Removing containers and networks..."
    docker-compose down --remove-orphans --volumes --timeout 10 2>/dev/null || true
    
    # Clean up any remaining containers related to the project
    log "Cleaning up any remaining SIMPREDE containers..."
    docker ps -a --filter "name=simprede" --format "{{.ID}}" | xargs -r docker rm -f 2>/dev/null || true
    docker ps -a --filter "name=airflow" --format "{{.ID}}" | xargs -r docker rm -f 2>/dev/null || true
    
    # Remove any dangling volumes from this project
    log "Cleaning up project volumes..."
    docker volume ls --filter "name=simprede" --format "{{.Name}}" | xargs -r docker volume rm 2>/dev/null || true
    
    # Kill any processes still using the Airflow port
    kill_port $AIRFLOW_PORT
    
    success "Docker containers stopped and cleaned up"
}

# Function to get Airflow credentials
get_airflow_credentials() {
    log "Extracting Airflow credentials..."
    
    cd "$SCRAPERS_DIR"
    
    # Default username
    AIRFLOW_USERNAME="admin"
    
    # Try to extract password from logs
    AIRFLOW_PASSWORD=$(docker-compose logs airflow-standalone 2>/dev/null | grep -i "Password for user 'admin'" | tail -1 | sed -n "s/.*Password for user 'admin': \([^ ]*\).*/\1/p" 2>/dev/null || echo "")
    
    # If not found in logs, try the generated file
    if [ -z "$AIRFLOW_PASSWORD" ]; then
        AIRFLOW_PASSWORD=$(docker-compose exec -T airflow-standalone cat /opt/airflow/simple_auth_manager_passwords.json.generated 2>/dev/null | grep -o '"admin": "[^"]*"' | cut -d'"' -f4 2>/dev/null || echo "")
    fi
    
    # If still not found, provide fallback message
    if [ -z "$AIRFLOW_PASSWORD" ]; then
        AIRFLOW_PASSWORD="<check logs with: docker-compose -f $SCRAPERS_DIR/docker-compose.yml logs airflow-standalone | grep -i password>"
    fi
}

# Function to start the scrapers container
start_scrapers() {
    log "Starting scrapers container (Airflow)..."
    
    cd "$SCRAPERS_DIR"
    
    # Check if Docker is available and running
    log "Checking Docker availability..."
    if ! command -v docker >/dev/null 2>&1; then
        error "Docker command not found. Please install Docker and try again."
        exit 1
    fi
    
    # Try multiple ways to check if Docker is running
    if ! docker version >/dev/null 2>&1 && ! docker ps >/dev/null 2>&1 && ! docker info >/dev/null 2>&1; then
        error "Docker daemon is not accessible."
        echo ""
        echo "🔍 This usually means Docker Desktop is not running."
        echo ""
        echo "📝 To fix this:"
        echo "   1. Open Docker Desktop application"
        echo "   2. Wait for the Docker whale icon to appear in your menu bar"
        echo "   3. Make sure Docker Desktop shows 'Engine running'"
        echo "   4. Try running the script again"
        echo ""
        echo "🔧 Alternative troubleshooting:"
        echo "   • Check if Docker works with sudo: sudo docker ps"
        echo "   • Add your user to docker group: sudo usermod -aG docker \$USER"
        echo "   • Restart Docker Desktop if it's already open"
        echo ""
        echo "💡 Run './run_local.sh docker-check' for detailed diagnostics"
        exit 1
    fi
    
    success "Docker is available and running"
    
    # Thoroughly stop any existing containers
    stop_docker_containers
    
    # Wait a moment for cleanup to complete
    sleep 3
    
    # Build and start the containers
    log "Building and starting scrapers containers..."
    docker-compose up --build -d
    
    # Wait for Airflow to be ready
    log "Waiting for Airflow to be ready..."
    local max_attempts=60
    local attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        if curl -s http://localhost:$AIRFLOW_PORT/api/v1/health >/dev/null 2>&1; then
            success "Airflow is ready and accessible on http://localhost:$AIRFLOW_PORT"
            break
        fi
        
        attempt=$((attempt + 1))
        if [ $((attempt % 10)) -eq 0 ]; then
            log "Still waiting for Airflow... (attempt $attempt/$max_attempts)"
        fi
        sleep 5
    done
    
    if [ $attempt -eq $max_attempts ]; then
        warning "Airflow health check timeout. It might still be starting up."
        log "Check logs with: docker-compose -f $SCRAPERS_DIR/docker-compose.yml logs -f"
    fi
    
    # Get Airflow credentials after startup
    get_airflow_credentials
}

# Function to stop all services
stop_services() {
    log "Stopping all services..."
    
    # Stop dashboard
    if [ -f "$DASHBOARD_DIR/dashboard.pid" ]; then
        local dashboard_pid=$(cat "$DASHBOARD_DIR/dashboard.pid")
        if ps -p $dashboard_pid > /dev/null; then
            log "Stopping dashboard (PID: $dashboard_pid)..."
            kill $dashboard_pid 2>/dev/null || true
        fi
        rm -f "$DASHBOARD_DIR/dashboard.pid"
    fi
    
    # Kill any remaining processes on dashboard port
    kill_port $DASHBOARD_PORT
    
    # Stop scrapers containers thoroughly
    stop_docker_containers
    
    success "All services stopped"
}

# Function to show status
show_status() {
    log "Checking service status..."
    
    echo ""
    echo "=== Service Status ==="
    
    # Check dashboard
    if check_port $DASHBOARD_PORT; then
        echo -e "${GREEN}✓${NC} Dashboard: Running on http://localhost:$DASHBOARD_PORT"
    else
        echo -e "${RED}✗${NC} Dashboard: Not running"
    fi
    
    # Check Airflow
    if check_port $AIRFLOW_PORT; then
        echo -e "${GREEN}✓${NC} Airflow: Running on http://localhost:$AIRFLOW_PORT"
        
        # Get credentials if Airflow is running
        get_airflow_credentials
        echo ""
        echo "=== Airflow Credentials ==="
        echo -e "${GREEN}Username:${NC} ${AIRFLOW_USERNAME:-admin}"
        echo -e "${GREEN}Password:${NC} ${AIRFLOW_PASSWORD:-<check logs>}"
    else
        echo -e "${RED}✗${NC} Airflow: Not running"
    fi
    
    # Check Docker containers
    cd "$SCRAPERS_DIR"
    echo ""
    echo "=== Docker Containers ==="
    docker-compose ps 2>/dev/null || echo "No containers running"
    
    echo ""
}

# Function to show logs
show_logs() {
    local service=$1
    
    case $service in
        "dashboard")
            log "Showing dashboard logs..."
            if [ -f "$DASHBOARD_DIR/dashboard.log" ]; then
                tail -f "$DASHBOARD_DIR/dashboard.log"
            else
                error "Dashboard log file not found"
            fi
            ;;
        "scrapers"|"airflow")
            log "Showing scrapers/Airflow logs..."
            cd "$SCRAPERS_DIR"
            docker-compose logs -f
            ;;
        *)
            error "Unknown service: $service. Use 'dashboard' or 'scrapers'"
            ;;
    esac
}

# Function to show usage
show_usage() {
    cat << EOF
SIMPREDE Local Development Orchestrator

Usage: $0 [COMMAND]

Commands:
    start           Start both dashboard and scrapers
    stop            Stop all services
    clean           Stop and clean up Docker containers and volumes
    restart         Restart all services
    status          Show status of all services
    credentials     Show Airflow login credentials (alias: creds)
    logs [service]  Show logs (service: dashboard, scrapers)
    setup           Setup dashboard environment only
    dashboard       Start only the dashboard
    scrapers        Start only the scrapers
    docker-check    Run Docker diagnostics (alias: check-docker)
    help            Show this help message

Examples:
    $0 start                    # Start both services
    $0 docker-check             # Diagnose Docker issues
    $0 clean                    # Clean up Docker containers/volumes
    $0 credentials              # Show Airflow credentials
    $0 logs dashboard          # Show dashboard logs
    $0 logs scrapers           # Show scrapers logs
    $0 status                  # Check service status

Ports:
    Dashboard (Streamlit): http://localhost:$DASHBOARD_PORT
    Airflow (Scrapers):   http://localhost:$AIRFLOW_PORT

Airflow Login:
    Default username: admin
    Password: Generated automatically (use '$0 credentials' to view)

Troubleshooting:
    If Docker errors occur, run: $0 docker-check

EOF
}

# Main script logic
case ${1:-start} in
    "start")
        log "Starting SIMPREDE local development environment..."
        setup_dashboard
        start_scrapers
        start_dashboard
        echo ""
        success "All services started successfully!"
        echo ""
        echo -e "${GREEN}🚀 SIMPREDE Services:${NC}"
        echo -e "   📊 Dashboard:  http://localhost:$DASHBOARD_PORT"
        echo -e "   🔄 Airflow:    http://localhost:$AIRFLOW_PORT"
        echo ""
        echo -e "${GREEN}🔐 Airflow Credentials:${NC}"
        echo -e "   Username: ${AIRFLOW_USERNAME:-admin}"
        echo -e "   Password: ${AIRFLOW_PASSWORD:-<check logs>}"
        echo ""
        echo -e "${YELLOW}💡 Useful commands:${NC}"
        echo -e "   Check status:  $0 status"
        echo -e "   View logs:     $0 logs [dashboard|scrapers]"
        echo -e "   Stop all:      $0 stop"
        ;;
    "stop")
        stop_services
        ;;
    "clean")
        log "Cleaning up Docker containers and volumes..."
        stop_docker_containers
        success "Docker cleanup completed"
        ;;
    "docker-check"|"check-docker")
        log "Running Docker diagnostics..."
        echo ""
        echo "=== Docker Diagnostics ==="
        
        # Check if docker command exists
        if command -v docker >/dev/null 2>&1; then
            echo -e "${GREEN}✓${NC} Docker command found: $(which docker)"
        else
            echo -e "${RED}✗${NC} Docker command not found"
            exit 1
        fi
        
        # Check Docker version
        echo -n "Docker version: "
        if docker --version 2>/dev/null; then
            echo -e "${GREEN}✓${NC} Docker version accessible"
        else
            echo -e "${RED}✗${NC} Cannot get Docker version"
        fi
        
        # Check Docker daemon connection
        echo -n "Docker daemon: "
        if docker info >/dev/null 2>&1; then
            echo -e "${GREEN}✓${NC} Docker daemon accessible"
        else
            echo -e "${RED}✗${NC} Docker daemon not accessible"
            echo "  Try: sudo docker info"
            echo "  Or check if Docker Desktop is running"
        fi
        
        # Check if we can list containers
        echo -n "Container listing: "
        if docker ps >/dev/null 2>&1; then
            echo -e "${GREEN}✓${NC} Can list containers"
            echo "Current containers:"
            docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || echo "None"
        else
            echo -e "${RED}✗${NC} Cannot list containers"
        fi
        
        # Check docker-compose
        echo -n "Docker Compose: "
        if command -v docker-compose >/dev/null 2>&1; then
            echo -e "${GREEN}✓${NC} docker-compose found: $(docker-compose --version)"
        elif docker compose version >/dev/null 2>&1; then
            echo -e "${GREEN}✓${NC} docker compose plugin found: $(docker compose version)"
        else
            echo -e "${RED}✗${NC} Neither docker-compose nor docker compose found"
        fi
        
        echo ""
        ;;
    "restart")
        log "Restarting all services..."
        stop_services
        sleep 3
        setup_dashboard
        start_scrapers
        start_dashboard
        success "All services restarted successfully!"
        echo ""
        echo -e "${GREEN}🔐 Airflow Credentials:${NC}"
        echo -e "   Username: ${AIRFLOW_USERNAME:-admin}"
        echo -e "   Password: ${AIRFLOW_PASSWORD:-<check logs>}"
        ;;
    "status")
        show_status
        ;;
    "credentials"|"creds")
        if check_port $AIRFLOW_PORT; then
            get_airflow_credentials
            echo ""
            echo -e "${GREEN}🔐 Airflow Credentials:${NC}"
            echo -e "   Username: ${AIRFLOW_USERNAME:-admin}"
            echo -e "   Password: ${AIRFLOW_PASSWORD:-<check logs>}"
            echo -e "   Web UI:   http://localhost:$AIRFLOW_PORT"
        else
            error "Airflow is not running. Start it first with: $0 start"
        fi
        ;;
    "logs")
        show_logs ${2:-scrapers}
        ;;
    "setup")
        setup_dashboard
        ;;
    "dashboard")
        setup_dashboard
        start_dashboard
        success "Dashboard started on http://localhost:$DASHBOARD_PORT"
        ;;
    "scrapers")
        start_scrapers
        success "Scrapers started on http://localhost:$AIRFLOW_PORT"
        echo ""
        echo -e "${GREEN}🔐 Airflow Credentials:${NC}"
        echo -e "   Username: ${AIRFLOW_USERNAME:-admin}"
        echo -e "   Password: ${AIRFLOW_PASSWORD:-<check logs>}"
        ;;
    "help"|"-h"|"--help")
        show_usage
        ;;
    *)
        error "Unknown command: $1"
        echo ""
        show_usage
        exit 1
        ;;
esac
