#!/bin/bash

# Orquestrador de Desenvolvimento Local SIMPREDE
# Este script executa tanto os contentores de scrapers (Airflow) como o dashboard localmente

set -e  # Sair em caso de erro

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # Sem Cor

# Configuração
PROJECT_ROOT="/Users/ruicarvalho/Desktop/projects/SIMPREDE"
SCRAPERS_DIR="$PROJECT_ROOT/Simprede_scrapers"
DASHBOARD_DIR="$PROJECT_ROOT/Simprede_dashboard"
DASHBOARD_VENV="$DASHBOARD_DIR/env"
DASHBOARD_PORT=8501
AIRFLOW_PORT=8080

# Função de registo
log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[ERRO]${NC} $1" >&2
}

success() {
    echo -e "${GREEN}[SUCESSO]${NC} $1"
}

warning() {
    echo -e "${YELLOW}[AVISO]${NC} $1"
}

# Função para verificar se uma porta está em uso
check_port() {
    local port=$1
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
        return 0  # Porta está em uso
    else
        return 1  # Porta está livre
    fi
}

# Função para terminar processos numa porta específica
kill_port() {
    local port=$1
    log "A verificar processos na porta $port..."
    if check_port $port; then
        warning "A porta $port está em uso. A terminar processos..."
        lsof -ti:$port | xargs kill -9 2>/dev/null || true
        sleep 2
    fi
}

# Função para configurar o ambiente do dashboard
setup_dashboard() {
    log "A configurar ambiente do dashboard..."
    
    cd "$DASHBOARD_DIR"
    
    # Verificar se o ambiente virtual existe
    if [ ! -d "$DASHBOARD_VENV" ]; then
        log "A criar ambiente virtual para o dashboard..."
        python3 -m venv env
    fi
    
    # Ativar ambiente virtual e instalar dependências
    log "A instalar dependências do dashboard..."
    source "$DASHBOARD_VENV/bin/activate"
    pip install --upgrade pip
    pip install -r requirements.txt
    
    success "Configuração do ambiente do dashboard concluída"
}

# Função para iniciar o dashboard
start_dashboard() {
    log "A iniciar dashboard Streamlit..."
    
    cd "$DASHBOARD_DIR"
    
    # Terminar processos existentes na porta do dashboard
    kill_port $DASHBOARD_PORT
    
    # Ativar ambiente virtual
    source "$DASHBOARD_VENV/bin/activate"
    
    # Iniciar Streamlit em segundo plano
    nohup streamlit run app.py --server.port=$DASHBOARD_PORT --server.address=0.0.0.0 > dashboard.log 2>&1 &
    DASHBOARD_PID=$!
    
    # Aguardar um momento e verificar se o processo iniciou com sucesso
    sleep 5
    if ps -p $DASHBOARD_PID > /dev/null; then
        success "Dashboard iniciado com sucesso em http://localhost:$DASHBOARD_PORT (PID: $DASHBOARD_PID)"
        echo $DASHBOARD_PID > dashboard.pid
    else
        error "Falha ao iniciar o dashboard. Verificar dashboard.log para detalhes."
        exit 1
    fi
}

# Função para parar contentores Docker completamente
stop_docker_containers() {
    log "A parar contentores Docker completamente..."
    
    cd "$SCRAPERS_DIR"
    
    # Verificar se o Docker está acessível antes de tentar parar contentores
    if ! docker ps >/dev/null 2>&1; then
        warning "Docker não está acessível - a saltar limpeza de contentores"
        kill_port $AIRFLOW_PORT
        return 0
    fi
    
    # Parar e remover contentores com timeout
    log "A parar contentores graciosamente..."
    docker-compose stop --timeout 30 2>/dev/null || true
    
    # Forçar remoção de contentores e redes
    log "A remover contentores e redes..."
    docker-compose down --remove-orphans --volumes --timeout 10 2>/dev/null || true
    
    # Limpar contentores restantes relacionados com o projeto
    log "A limpar contentores SIMPREDE restantes..."
    docker ps -a --filter "name=simprede" --format "{{.ID}}" | xargs -r docker rm -f 2>/dev/null || true
    docker ps -a --filter "name=airflow" --format "{{.ID}}" | xargs -r docker rm -f 2>/dev/null || true
    
    # Remover volumes pendentes deste projeto
    log "A limpar volumes do projeto..."
    docker volume ls --filter "name=simprede" --format "{{.Name}}" | xargs -r docker volume rm 2>/dev/null || true
    
    # Terminar processos que ainda usem a porta do Airflow
    kill_port $AIRFLOW_PORT
    
    success "Contentores Docker parados e limpos"
}

# Função para obter credenciais do Airflow
get_airflow_credentials() {
    log "A extrair credenciais do Airflow..."
    
    cd "$SCRAPERS_DIR"
    
    # Nome de utilizador padrão
    AIRFLOW_USERNAME="admin"
    
    # Tentar extrair password dos logs
    AIRFLOW_PASSWORD=$(docker-compose logs airflow-standalone 2>/dev/null | grep -i "Password for user 'admin'" | tail -1 | sed -n "s/.*Password for user 'admin': \([^ ]*\).*/\1/p" 2>/dev/null || echo "")
    
    # Se não encontrado nos logs, tentar o ficheiro gerado
    if [ -z "$AIRFLOW_PASSWORD" ]; then
        AIRFLOW_PASSWORD=$(docker-compose exec -T airflow-standalone cat /opt/airflow/simple_auth_manager_passwords.json.generated 2>/dev/null | grep -o '"admin": "[^"]*"' | cut -d'"' -f4 2>/dev/null || echo "")
    fi
    
    # Se ainda não encontrado, fornecer mensagem de fallback
    if [ -z "$AIRFLOW_PASSWORD" ]; then
        AIRFLOW_PASSWORD="<verificar logs com: docker-compose -f $SCRAPERS_DIR/docker-compose.yml logs airflow-standalone | grep -i password>"
    fi
}

# Função para iniciar o contentor de scrapers
start_scrapers() {
    log "A iniciar contentor de scrapers (Airflow)..."
    
    cd "$SCRAPERS_DIR"
    
    # Verificar se o Docker está disponível e em execução
    log "A verificar disponibilidade do Docker..."
    if ! command -v docker >/dev/null 2>&1; then
        error "Comando Docker não encontrado. Por favor instale Docker e tente novamente."
        exit 1
    fi
    
    # Tentar várias formas de verificar se o Docker está em execução
    if ! docker version >/dev/null 2>&1 && ! docker ps >/dev/null 2>&1 && ! docker info >/dev/null 2>&1; then
        error "O daemon Docker não está acessível."
        echo ""
        echo "🔍 Isto normalmente significa que o Docker Desktop não está em execução."
        echo ""
        echo "📝 Para corrigir isto:"
        echo "   1. Abra a aplicação Docker Desktop"
        echo "   2. Aguarde que o ícone da baleia do Docker apareça na barra de menu"
        echo "   3. Certifique-se que o Docker Desktop mostra 'Engine running'"
        echo "   4. Tente executar o script novamente"
        echo ""
        echo "🔧 Resolução de problemas alternativa:"
        echo "   • Verifique se o Docker funciona com sudo: sudo docker ps"
        echo "   • Adicione o seu utilizador ao grupo docker: sudo usermod -aG docker \$USER"
        echo "   • Reinicie o Docker Desktop se já estiver aberto"
        echo ""
        echo "💡 Execute './run_local.sh docker-check' para diagnósticos detalhados"
        exit 1
    fi
    
    success "Docker está disponível e em execução"
    
    # Parar completamente quaisquer contentores existentes
    stop_docker_containers
    
    # Aguardar um momento para a limpeza terminar
    sleep 3
    
    # Construir e iniciar os contentores
    log "A construir e iniciar contentores de scrapers..."
    docker-compose up --build -d
    
    # Aguardar que o Airflow esteja pronto
    log "A aguardar que o Airflow esteja pronto..."
    local max_attempts=60
    local attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        if curl -s http://localhost:$AIRFLOW_PORT/api/v1/health >/dev/null 2>&1; then
            success "Airflow está pronto e acessível em http://localhost:$AIRFLOW_PORT"
            break
        fi
        
        attempt=$((attempt + 1))
        if [ $((attempt % 10)) -eq 0 ]; then
            log "Ainda a aguardar o Airflow... (tentativa $attempt/$max_attempts)"
        fi
        sleep 5
    done
    
    if [ $attempt -eq $max_attempts ]; then
        warning "Timeout na verificação de saúde do Airflow. Pode ainda estar a iniciar."
        log "Verificar logs com: docker-compose -f $SCRAPERS_DIR/docker-compose.yml logs -f"
    fi
    
    # Obter credenciais do Airflow após o arranque
    get_airflow_credentials
}

# Função para parar todos os serviços
stop_services() {
    log "A parar todos os serviços..."
    
    # Parar dashboard
    if [ -f "$DASHBOARD_DIR/dashboard.pid" ]; then
        local dashboard_pid=$(cat "$DASHBOARD_DIR/dashboard.pid")
        if ps -p $dashboard_pid > /dev/null; then
            log "A parar dashboard (PID: $dashboard_pid)..."
            kill $dashboard_pid 2>/dev/null || true
        fi
        rm -f "$DASHBOARD_DIR/dashboard.pid"
    fi
    
    # Terminar processos restantes na porta do dashboard
    kill_port $DASHBOARD_PORT
    
    # Parar contentores de scrapers completamente
    stop_docker_containers
    
    success "Todos os serviços parados"
}

# Função para mostrar estado
show_status() {
    log "A verificar estado dos serviços..."
    
    echo ""
    echo "=== Estado dos Serviços ==="
    
    # Verificar dashboard
    if check_port $DASHBOARD_PORT; then
        echo -e "${GREEN}✓${NC} Dashboard: Em execução em http://localhost:$DASHBOARD_PORT"
    else
        echo -e "${RED}✗${NC} Dashboard: Não está em execução"
    fi
    
    # Verificar Airflow
    if check_port $AIRFLOW_PORT; then
        echo -e "${GREEN}✓${NC} Airflow: Em execução em http://localhost:$AIRFLOW_PORT"
        
        # Obter credenciais se o Airflow estiver em execução
        get_airflow_credentials
        echo ""
        echo "=== Credenciais do Airflow ==="
        echo -e "${GREEN}Utilizador:${NC} ${AIRFLOW_USERNAME:-admin}"
        echo -e "${GREEN}Password:${NC} ${AIRFLOW_PASSWORD:-<verificar logs>}"
    else
        echo -e "${RED}✗${NC} Airflow: Não está em execução"
    fi
    
    # Verificar contentores Docker
    cd "$SCRAPERS_DIR"
    echo ""
    echo "=== Contentores Docker ==="
    docker-compose ps 2>/dev/null || echo "Nenhum contentor em execução"
    
    echo ""
}

# Função para mostrar logs
show_logs() {
    local service=$1
    
    case $service in
        "dashboard")
            log "A mostrar logs do dashboard..."
            if [ -f "$DASHBOARD_DIR/dashboard.log" ]; then
                tail -f "$DASHBOARD_DIR/dashboard.log"
            else
                error "Ficheiro de log do dashboard não encontrado"
            fi
            ;;
        "scrapers"|"airflow")
            log "A mostrar logs dos scrapers/Airflow..."
            cd "$SCRAPERS_DIR"
            docker-compose logs -f
            ;;
        *)
            error "Serviço desconhecido: $service. Use 'dashboard' ou 'scrapers'"
            ;;
    esac
}

# Função para mostrar utilização
show_usage() {
    cat << EOF
Orquestrador de Desenvolvimento Local SIMPREDE

Utilização: $0 [COMANDO]

Comandos:
    start           Iniciar dashboard e scrapers
    stop            Parar todos os serviços
    clean           Parar e limpar contentores e volumes Docker
    restart         Reiniciar todos os serviços
    status          Mostrar estado de todos os serviços
    credentials     Mostrar credenciais de login do Airflow (alias: creds)
    logs [serviço]  Mostrar logs (serviço: dashboard, scrapers)
    setup           Configurar apenas o ambiente do dashboard
    dashboard       Iniciar apenas o dashboard
    scrapers        Iniciar apenas os scrapers
    docker-check    Executar diagnósticos Docker (alias: check-docker)
    help            Mostrar esta mensagem de ajuda

Exemplos:
    $0 start                    # Iniciar ambos os serviços
    $0 docker-check             # Diagnosticar problemas do Docker
    $0 clean                    # Limpar contentores/volumes Docker
    $0 credentials              # Mostrar credenciais do Airflow
    $0 logs dashboard          # Mostrar logs do dashboard
    $0 logs scrapers           # Mostrar logs dos scrapers
    $0 status                  # Verificar estado dos serviços

Portas:
    Dashboard (Streamlit): http://localhost:$DASHBOARD_PORT
    Airflow (Scrapers):   http://localhost:$AIRFLOW_PORT

Login do Airflow:
    Utilizador padrão: admin
    Password: Gerada automaticamente (use '$0 credentials' para ver)

Resolução de problemas:
    Se ocorrerem erros do Docker, execute: $0 docker-check

EOF
}

# Lógica principal do script
case ${1:-start} in
    "start")
        log "A iniciar ambiente de desenvolvimento local SIMPREDE..."
        setup_dashboard
        start_scrapers
        start_dashboard
        echo ""
        success "Todos os serviços iniciados com sucesso!"
        echo ""
        echo -e "${GREEN}🚀 Serviços SIMPREDE:${NC}"
        echo -e "   📊 Dashboard:  http://localhost:$DASHBOARD_PORT"
        echo -e "   🔄 Airflow:    http://localhost:$AIRFLOW_PORT"
        echo ""
        echo -e "${GREEN}🔐 Credenciais do Airflow:${NC}"
        echo -e "   Utilizador: ${AIRFLOW_USERNAME:-admin}"
        echo -e "   Password: ${AIRFLOW_PASSWORD:-<verificar logs>}"
        echo ""
        echo -e "${YELLOW}💡 Comandos úteis:${NC}"
        echo -e "   Verificar estado:  $0 status"
        echo -e "   Ver logs:          $0 logs [dashboard|scrapers]"
        echo -e "   Parar tudo:        $0 stop"
        ;;
    "stop")
        stop_services
        ;;
    "clean")
        log "A limpar contentores e volumes Docker..."
        stop_docker_containers
        success "Limpeza Docker concluída"
        ;;
    "docker-check"|"check-docker")
        log "A executar diagnósticos Docker..."
        echo ""
        echo "=== Diagnósticos Docker ==="
        
        # Verificar se o comando docker existe
        if command -v docker >/dev/null 2>&1; then
            echo -e "${GREEN}✓${NC} Comando Docker encontrado: $(which docker)"
        else
            echo -e "${RED}✗${NC} Comando Docker não encontrado"
            exit 1
        fi
        
        # Verificar versão do Docker
        echo -n "Versão do Docker: "
        if docker --version 2>/dev/null; then
            echo -e "${GREEN}✓${NC} Versão do Docker acessível"
        else
            echo -e "${RED}✗${NC} Não é possível obter a versão do Docker"
        fi
        
        # Verificar ligação ao daemon Docker
        echo -n "Daemon Docker: "
        if docker info >/dev/null 2>&1; then
            echo -e "${GREEN}✓${NC} Daemon Docker acessível"
        else
            echo -e "${RED}✗${NC} Daemon Docker não acessível"
            echo "  Tente: sudo docker info"
            echo "  Ou verifique se o Docker Desktop está em execução"
        fi
        
        # Verificar se conseguimos listar contentores
        echo -n "Listagem de contentores: "
        if docker ps >/dev/null 2>&1; then
            echo -e "${GREEN}✓${NC} Consegue listar contentores"
            echo "Contentores atuais:"
            docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || echo "Nenhum"
        else
            echo -e "${RED}✗${NC} Não consegue listar contentores"
        fi
        
        # Verificar docker-compose
        echo -n "Docker Compose: "
        if command -v docker-compose >/dev/null 2>&1; then
            echo -e "${GREEN}✓${NC} docker-compose encontrado: $(docker-compose --version)"
        elif docker compose version >/dev/null 2>&1; then
            echo -e "${GREEN}✓${NC} plugin docker compose encontrado: $(docker compose version)"
        else
            echo -e "${RED}✗${NC} Nem docker-compose nem docker compose encontrados"
        fi
        
        echo ""
        ;;
    "restart")
        log "A reiniciar todos os serviços..."
        stop_services
        sleep 3
        setup_dashboard
        start_scrapers
        start_dashboard
        success "Todos os serviços reiniciados com sucesso!"
        echo ""
        echo -e "${GREEN}🔐 Credenciais do Airflow:${NC}"
        echo -e "   Utilizador: ${AIRFLOW_USERNAME:-admin}"
        echo -e "   Password: ${AIRFLOW_PASSWORD:-<verificar logs>}"
        ;;
    "status")
        show_status
        ;;
    "credentials"|"creds")
        if check_port $AIRFLOW_PORT; then
            get_airflow_credentials
            echo ""
            echo -e "${GREEN}🔐 Credenciais do Airflow:${NC}"
            echo -e "   Utilizador: ${AIRFLOW_USERNAME:-admin}"
            echo -e "   Password: ${AIRFLOW_PASSWORD:-<verificar logs>}"
            echo -e "   Interface Web: http://localhost:$AIRFLOW_PORT"
        else
            error "Airflow não está em execução. Inicie-o primeiro com: $0 start"
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
        success "Dashboard iniciado em http://localhost:$DASHBOARD_PORT"
        ;;
    "scrapers")
        start_scrapers
        success "Scrapers iniciados em http://localhost:$AIRFLOW_PORT"
        echo ""
        echo -e "${GREEN}🔐 Credenciais do Airflow:${NC}"
        echo -e "   Utilizador: ${AIRFLOW_USERNAME:-admin}"
        echo -e "   Password: ${AIRFLOW_PASSWORD:-<verificar logs>}"
        ;;
    "help"|"-h"|"--help")
        show_usage
        ;;
    *)
        error "Comando desconhecido: $1"
        echo ""
        show_usage
        exit 1
        ;;
esac
