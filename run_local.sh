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

# Function to start the scrapers container
start_scrapers() {
    log "Starting scrapers container (Airflow)..."
    
    cd "$SCRAPERS_DIR"
    
    # Kill any existing processes on Airflow port
    kill_port $AIRFLOW_PORT
    
    # Check if Docker is running
    if ! docker info >/dev/null 2>&1; then
        error "Docker is not running. Please start Docker and try again."
        exit 1
    fi
    
    # Stop any existing containers
    log "Stopping any existing scrapers containers..."
    docker-compose down --remove-orphans 2>/dev/null || true
    
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
    
    # Stop scrapers containers
    cd "$SCRAPERS_DIR"
    log "Stopping scrapers containers..."
    docker-compose down
    
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
    restart         Restart all services
    status          Show status of all services
    logs [service]  Show logs (service: dashboard, scrapers)
    setup           Setup dashboard environment only
    dashboard       Start only the dashboard
    scrapers        Start only the scrapers
    help            Show this help message

Examples:
    $0 start                    # Start both services
    $0 logs dashboard          # Show dashboard logs
    $0 logs scrapers           # Show scrapers logs
    $0 status                  # Check service status

Ports:
    Dashboard (Streamlit): http://localhost:$DASHBOARD_PORT
    Airflow (Scrapers):   http://localhost:$AIRFLOW_PORT

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
        echo -e "${YELLOW}💡 Useful commands:${NC}"
        echo -e "   Check status:  $0 status"
        echo -e "   View logs:     $0 logs [dashboard|scrapers]"
        echo -e "   Stop all:      $0 stop"
        ;;
    "stop")
        stop_services
        ;;
    "restart")
        log "Restarting all services..."
        stop_services
        sleep 3
        setup_dashboard
        start_scrapers
        start_dashboard
        success "All services restarted successfully!"
        ;;
    "status")
        show_status
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
