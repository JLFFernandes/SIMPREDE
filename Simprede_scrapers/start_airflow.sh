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
        DOCKER_COMPOSE_VERSION=$(docker-compose --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)
    elif docker compose version > /dev/null 2>&1; then
        DOCKER_COMPOSE_CMD="docker compose"
        DOCKER_COMPOSE_VERSION=$(docker compose version --short 2>/dev/null || echo "2.0.0")
    else
        print_error "Neither 'docker-compose' nor 'docker compose' is available. Please install Docker Compose."
        exit 1
    fi
    print_success "Found Docker Compose: $DOCKER_COMPOSE_CMD (version $DOCKER_COMPOSE_VERSION)"
}

# Function to set proper permissions
set_permissions() {
    print_info "Configuração de permissões dos diretórios..."
    
    # Create directories if they don't exist
    mkdir -p logs dags data scripts config
    
    # Set AIRFLOW_UID environment variable
    export AIRFLOW_UID=$(id -u)
    
    # Safely update .env file without overwriting existing content
    if [ -f ".env" ]; then
        # Check if AIRFLOW_UID already exists in .env
        if grep -q "^AIRFLOW_UID=" .env; then
            # Update existing AIRFLOW_UID (fix for macOS sed)
            if [[ "$OSTYPE" == "darwin"* ]]; then
                sed -i '' "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$AIRFLOW_UID/" .env
            else
                sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$AIRFLOW_UID/" .env
            fi
            print_info "AIRFLOW_UID atualizado no ficheiro .env existente"
        else
            # Append AIRFLOW_UID to existing .env
            echo "AIRFLOW_UID=$AIRFLOW_UID" >> .env
            print_info "AIRFLOW_UID adicionado ao ficheiro .env existente"
        fi
    else
        # Create new .env with only AIRFLOW_UID
        echo "AIRFLOW_UID=$AIRFLOW_UID" > .env
        print_info "Ficheiro .env criado com AIRFLOW_UID"
    fi
    
    print_success "Permissões configuradas (AIRFLOW_UID=$AIRFLOW_UID)"
}

# Function to setup GCS configuration
setup_gcs_config() {
    print_info "Configuração do Google Cloud Storage..."
    
    # Create .env file from template if it doesn't exist
    if [ ! -f ".env" ]; then
        if [ -f ".env.template" ]; then
            cp ".env.template" ".env"
            print_success "Ficheiro .env criado a partir do template"
        else
            # Create basic .env with GCS defaults
            cat > ".env" << EOF
# SIMPREDE Airflow Environment Configuration
AIRFLOW_UID=$(id -u)

# GCS Configuration
GCS_PROJECT_ID=simprede
GCS_BUCKET_NAME=simprede-data-pipeline
GCS_LOCATION=EUROPE-WEST1
GOOGLE_APPLICATION_CREDENTIALS=./config/gcs-credentials.json
GCS_DEBUG=false
EOF
            print_success "Ficheiro .env criado com configurações GCS padrão"
        fi
    fi
    
    # Ensure GCS environment variables are set in .env if missing
    local gcs_vars=("GCS_PROJECT_ID=simprede-461309" "GCS_BUCKET_NAME=simprede-data-pipeline" "GCS_LOCATION=EUROPE-WEST1" "GCS_DEBUG=false")
    
    for var in "${gcs_vars[@]}"; do
        local var_name=$(echo "$var" | cut -d'=' -f1)
        if ! grep -q "^${var_name}=" .env; then
            echo "$var" >> .env
            print_info "Adicionado $var_name ao .env"
        fi
    done
    
    # Check for GCS credentials
    if [ -f "./config/gcs-credentials.json" ]; then
        print_success "Credenciais GCS encontradas: ./config/gcs-credentials.json"
        # Validate JSON
        if python3 -c "import json; json.load(open('./config/gcs-credentials.json'))" 2>/dev/null; then
            print_success "Ficheiro de credenciais GCS é válido"
        else
            print_warning "Ficheiro de credenciais GCS tem formato inválido"
        fi
    else
        print_info "Credenciais GCS não encontradas - usando Application Default Credentials"
        print_info "O sistema tentará usar:"
        print_info "1. Credenciais de utilizador (gcloud auth application-default login)"
        print_info "2. Variáveis de ambiente GOOGLE_APPLICATION_CREDENTIALS"
        print_info "3. Credenciais da máquina virtual (se executar no GCP)"
        echo ""
        print_info "Para usar credenciais explícitas (opcional):"
        print_info "1. Vá ao Google Cloud Console (https://console.cloud.google.com/)"
        print_info "2. Crie uma conta de serviço com roles Storage Admin"
        print_info "3. Baixe o ficheiro JSON e salve como: ./config/gcs-credentials.json"
        echo ""
        print_success "O export GCS funcionará automaticamente com suas credenciais de utilizador!"
        echo ""
    fi
}

# Function to create necessary directories
create_directories() {
    print_info "Criação dos diretórios necessários..."
    
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
    
    print_success "Diretórios criados com sucesso"
}

# Function to build and start containers
start_containers() {
    print_info "Construção e arranque dos contentores Airflow..."
    
    # Check if we need to rebuild (when Dockerfile or requirements change)
    REBUILD_NEEDED=false
    if [ ! -f ".airflow_built" ] || [ "Dockerfile" -nt ".airflow_built" ] || [ "requirements.txt" -nt ".airflow_built" ]; then
        print_warning "Detectadas alterações no Dockerfile ou requirements.txt"
        read -p "Reconstruir imagem Docker? (recomendado) (Y/n): " -r
        if [[ ! $REPLY =~ ^[Nn]$ ]]; then
            REBUILD_NEEDED=true
            print_info "Imagem será reconstruída..."
        fi
    fi
    
    # Set AIRFLOW_UID if not already set
    if [ -z "${AIRFLOW_UID}" ]; then
        export AIRFLOW_UID=$(id -u)
        
        # Safely append to .env if it doesn't already contain AIRFLOW_UID
        if [ -f ".env" ]; then
            if ! grep -q "^AIRFLOW_UID=" .env; then
                echo "AIRFLOW_UID=${AIRFLOW_UID}" >> .env
                print_info "AIRFLOW_UID definido como ${AIRFLOW_UID} e adicionado ao .env"
            fi
        else
            echo "AIRFLOW_UID=${AIRFLOW_UID}" > .env
            print_info "Ficheiro .env criado com AIRFLOW_UID=${AIRFLOW_UID}"
        fi
    fi
    
    # Verify .env content before starting
    print_info "Verificação do conteúdo do ficheiro .env:"
    if [ -f ".env" ]; then
        grep -v "PASSWORD" .env | while read line; do
            if [ ! -z "$line" ] && [[ ! "$line" =~ ^#.* ]]; then
                print_info "  $line"
            fi
        done
        
        # Check for database credentials without showing them
        if grep -q "^DB_HOST=" .env; then
            print_success "Credenciais da base de dados preservadas no .env"
        else
            print_warning "Credenciais da base de dados não encontradas no .env"
        fi
    else
        print_warning "Ficheiro .env não encontrado"
    fi
    
    # Build and start containers with rebuild if needed
    if [ "$REBUILD_NEEDED" = true ]; then
        print_info "Rebuilding Docker images..."
        
        # Use different commands based on docker-compose version and availability
        if command -v docker-compose > /dev/null 2>&1; then
            # For docker-compose, build separately then start
            docker-compose build airflow-standalone
            print_info "Starting containers with rebuilt image..."
            docker-compose up -d
        elif docker compose version > /dev/null 2>&1; then
            # For docker compose, use --no-cache flag only if supported
            if docker compose build --help 2>/dev/null | grep -q "\-\-no-cache"; then
                docker compose build --no-cache airflow-standalone
            else
                docker compose build airflow-standalone
            fi
            print_info "Starting containers with rebuilt image..."
            docker compose up -d
        fi
    else
        print_info "Starting containers..."
        $DOCKER_COMPOSE_CMD up -d
    fi
    
    # Mark as built
    touch .airflow_built
    
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
            break
        fi
        counter=$((counter + 2))
        sleep 2
    done
    
    if [ $counter -ge $timeout ]; then
        print_warning "PostgreSQL did not become ready within timeout"
    else
        print_success "PostgreSQL is ready"
    fi
    
    # Wait for Airflow webserver
    print_info "Waiting for Airflow webserver to be ready..."
    timeout=120
    counter=0
    
    while [ $counter -lt $timeout ]; do
        if curl -s http://localhost:8080/health > /dev/null 2>&1; then
            break
        fi
        counter=$((counter + 5))
        sleep 5
    done
    
    if [ $counter -ge $timeout ]; then
        print_warning "Airflow webserver did not become ready within timeout"
    else
        print_success "Airflow webserver is ready"
    fi
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
    echo -e "${YELLOW}Test database providers:${NC}"
    echo "  chmod +x scripts/test_providers.sh && ./scripts/test_providers.sh"
    echo ""
    echo -e "${YELLOW}Install database providers:${NC}"
    echo "  ./install_providers.sh"
    echo ""
    echo -e "${YELLOW}View logs:${NC}"
    echo "  $DOCKER_COMPOSE_CMD logs airflow-standalone"
    echo "  $DOCKER_COMPOSE_CMD logs postgres"
    echo ""
    echo -e "${YELLOW}Stop services:${NC}"
    echo "  $DOCKER_COMPOSE_CMD down"
    echo ""
    echo ""
    echo -e "${YELLOW}Access container shell:${NC}"
    echo "  $DOCKER_COMPOSE_CMD exec airflow-standalone bash"
    echo ""
    echo -e "${YELLOW}Validate GCS setup:${NC}"
    echo "  $DOCKER_COMPOSE_CMD exec airflow-standalone python3 /opt/airflow/scripts/google_scraper/exportador_gcs/validate_gcs_setup.py"
    echo ""
    echo -e "${YELLOW}Force rebuild:${NC}"
    echo "  $DOCKER_COMPOSE_CMD down && $DOCKER_COMPOSE_CMD build --no-cache && $DOCKER_COMPOSE_CMD up -d"
}

# Function to display GCS status
display_gcs_status() {
    print_separator
    print_info "GOOGLE CLOUD STORAGE STATUS"
    print_separator
    
    if [ -f "./config/gcs-credentials.json" ]; then
        print_success "✅ Credenciais GCS: Configuradas (Service Account)"
        
        # Extract project info if possible
        if command -v python3 > /dev/null 2>&1; then
            PROJECT_ID=$(python3 -c "import json; data=json.load(open('./config/gcs-credentials.json')); print(data.get('project_id', 'N/A'))" 2>/dev/null || echo "N/A")
            if [ "$PROJECT_ID" != "N/A" ]; then
                print_info "   Project ID: $PROJECT_ID"
            fi
        fi
        
        # Get GCS settings from .env
        if [ -f ".env" ]; then
            BUCKET_NAME=$(grep "^GCS_BUCKET_NAME=" .env | cut -d'=' -f2 || echo "simprede-data-pipeline")
            print_info "   Bucket: $BUCKET_NAME"
            print_success "   Export GCS: ATIVO (Service Account)"
        fi
    else
        print_success "✅ Credenciais GCS: Application Default Credentials"
        print_info "   Project ID: simprede-461309"
        if [ -f ".env" ]; then
            BUCKET_NAME=$(grep "^GCS_BUCKET_NAME=" .env | cut -d'=' -f2 || echo "simprede-data-pipeline")
            print_info "   Bucket: $BUCKET_NAME"
            print_success "   Export GCS: ATIVO (Credenciais de Utilizador)"
        fi
        print_info "   Usando: Suas credenciais GCP existentes"
    fi
}

# Function to display access information
display_access_info() {
    print_separator
    print_success "SIMPREDE Airflow started successfully!"
    print_separator
    
    print_info "Access Information:"
    print_info "  Web UI: http://localhost:8080"
    print_info "  Default credentials: admin/admin"
    print_info ""
    
    print_info "Useful commands:"
    print_info "  View logs: $DOCKER_COMPOSE_CMD logs -f"
    print_info "  Stop services: ./stop_airflow.sh"
    print_info "  Restart services: ./restart_airflow.sh"
    print_separator
}

# Main execution
main() {
    print_separator
    print_info "Starting SIMPREDE Airflow Environment"
    print_separator
    
    # Check if we're in the correct directory
    if [ ! -f "docker-compose.yml" ]; then
        print_error "docker-compose.yml not found. Make sure you're in the correct directory."
        exit 1
    fi
    
    # Check for project root .env file
    PROJECT_ROOT="$(dirname "$(pwd)")"
    if [ -f "$PROJECT_ROOT/.env" ]; then
        print_info "Found .env file in project root: $PROJECT_ROOT/.env"
        # Copy to local directory if not exists or different
        if [ ! -f ".env" ] || ! cmp -s "$PROJECT_ROOT/.env" ".env"; then
            cp "$PROJECT_ROOT/.env" ".env"
            print_info "Copied .env from project root"
        fi
    elif [ ! -f ".env" ]; then
        print_warning "No .env file found in project root or current directory"
        print_info "You may need to create one from .env.template"
    fi
    
    check_docker
    check_docker_compose
    set_permissions
    setup_gcs_config
    create_directories
    start_containers
    wait_for_services
    display_access_info
}

# Handle script interruption
trap 'print_error "Script interrupted. You may need to run: $DOCKER_COMPOSE_CMD down"; exit 1' INT TERM

# Run main function
main "$@"
}

# Handle script interruption
trap 'print_error "Script interrupted. You may need to run: $DOCKER_COMPOSE_CMD down"; exit 1' INT TERM

# Run main function
main "$@"
