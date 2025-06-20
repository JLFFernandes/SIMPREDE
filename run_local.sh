#!/bin/bash

# Orquestrador de Desenvolvimento Local SIMPREDE
# Este script executa tanto os containers de scrapers (Airflow) como o dashboard localmente

set -e  # Sair em caso de erro

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # Sem Cor

# Configuração
PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
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

# Função para setup do ambiente Python
setup_python_env() {
    local env_path="$1"
    local requirements_file="$2"
    
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Verificando ambiente virtual em: $env_path"
    
    # Verificar se o módulo venv está instalado
    if ! python3 -c "import venv" >/dev/null 2>&1; then
        echo "[ERRO] Módulo 'venv' do Python3 não está instalado."
        echo "Por favor, instale com: sudo apt-get install python3-venv"
        exit 1
    fi
    
    # Criar diretório pai se não existir
    mkdir -p "$(dirname "$env_path")"
    
    # Verificar se o ambiente existe e está corretamente configurado
    if [ -d "$env_path" ] && [ ! -f "$env_path/bin/activate" ]; then
        echo "[AVISO] O ambiente virtual existe mas parece estar corrompido (falta bin/activate)"
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Removendo ambiente virtual corrompido..."
        rm -rf "$env_path"
        echo "[SUCESSO] Ambiente virtual removido para recriação"
    fi
    
    # Criar ambiente virtual se não existir ou foi removido
    if [ ! -d "$env_path" ]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] A criar ambiente virtual Python em: $env_path"
        python3 -m venv "$env_path"
        
        # Verificar se a criação foi bem-sucedida
        if [ $? -ne 0 ]; then
            echo "[ERRO] Falha ao criar ambiente virtual"
            echo "Tentando diagnosticar o problema..."
            python3 --version
            echo "Diretório de destino ($env_path) permissões:"
            ls -ld "$(dirname "$env_path")"
            echo "Tentando criar com -m venv verboso:"
            python3 -v -m venv "$env_path"
            exit 1
        fi
        
        # Verificar se os arquivos esperados foram criados
        if [ ! -f "$env_path/bin/activate" ]; then
            echo "[ERRO] Arquivo de ativação não foi criado em $env_path/bin/activate"
            echo "Conteúdo do diretório $env_path:"
            ls -la "$env_path"
            echo "Conteúdo do diretório $env_path/bin (se existir):"
            ls -la "$env_path/bin" 2>/dev/null || echo "Diretório bin não existe"
            exit 1
        fi
        
        echo "[SUCESSO] Ambiente virtual criado em $env_path"
    else
        echo "[INFO] Usando ambiente virtual existente em: $env_path"
    fi
    
    # Verificar se o arquivo de ativação existe antes de tentar usá-lo
    if [ ! -f "$env_path/bin/activate" ]; then
        echo "[ERRO] Arquivo de ativação não encontrado em $env_path/bin/activate"
        echo "O ambiente virtual parece estar corrompido. Execute o script novamente para remover e recriar."
        exit 1
    fi
    
    # Ativar ambiente virtual
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Ativando ambiente virtual: source $env_path/bin/activate"
    source "$env_path/bin/activate"
    
    if [ $? -ne 0 ]; then
        echo "[ERRO] Falha ao ativar ambiente virtual"
        exit 1
    fi
    
    # Verificar se a ativação funcionou
    if [ -z "$VIRTUAL_ENV" ]; then
        echo "[ERRO] Ambiente virtual não foi ativado corretamente"
        exit 1
    fi
    
    echo "[SUCESSO] Ambiente virtual ativado: $VIRTUAL_ENV"
    
    # Instalar/atualizar requisitos se o ficheiro existir
    if [ -f "$requirements_file" ]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] A instalar/atualizar dependências Python..."
        pip install --upgrade pip
        pip install -r "$requirements_file"
        if [ $? -ne 0 ]; then
            echo "[ERRO] Falha ao instalar dependências"
            exit 1
        fi
        echo "[SUCESSO] Dependências instaladas"
    fi
}

# Função para configurar o ambiente do dashboard
setup_dashboard() {
    log "A configurar ambiente do dashboard..."
    
    cd "$DASHBOARD_DIR"
    
    # Verificar se Python3 está disponível
    if ! command -v python3 >/dev/null 2>&1; then
        error "Python3 não encontrado. Por favor instale Python3 e tente novamente."
        exit 1
    fi
    
    # Verificar se requirements.txt existe
    if [ ! -f "requirements.txt" ]; then
        error "Ficheiro requirements.txt não encontrado em $DASHBOARD_DIR"
        exit 1
    fi
    
    # Usar a função setup_python_env para configurar o ambiente
    setup_python_env "$DASHBOARD_VENV" "requirements.txt"
    
    success "Configuração do ambiente do dashboard concluída"
}

# Função para iniciar o dashboard
start_dashboard() {
    log "A iniciar dashboard Streamlit..."
    
    cd "$DASHBOARD_DIR"
    
    # Usar setup_python_env para garantir que o ambiente está configurado
    setup_python_env "$DASHBOARD_VENV" "requirements.txt"

    # Terminar processos existentes na porta do dashboard
    kill_port $DASHBOARD_PORT
    
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

# Função para verificar e configurar .env
setup_env_file() {
    log "A verificar configuração do ficheiro .env..."

    # Verificar se o .env existe na raiz do projeto
    if [ ! -f "$PROJECT_ROOT/.env" ]; then
        if [ -f "$PROJECT_ROOT/.env.template" ]; then
            warning "Ficheiro .env não encontrado na raiz do projeto. A criar a partir do template..."
            cp "$PROJECT_ROOT/.env.template" "$PROJECT_ROOT/.env"
            echo ""
            echo -e "${YELLOW}IMPORTANTE:${NC} Foi criado um ficheiro .env a partir do template."
            echo "Por favor, edite $PROJECT_ROOT/.env e preencha as configurações necessárias antes de continuar."
            echo ""
            echo "Configurações obrigatórias:"
            echo "  - DB_USER, DB_PASSWORD, DB_HOST (credenciais da base de dados)"
            echo "  - GCS_PROJECT_ID, GCS_BUCKET_NAME (configuração Google Cloud)"
            echo ""
            read -p "Pressione Enter depois de configurar o ficheiro .env..." -r
        else
            # Use a more portable way to get relative path
            local rel_project_root
            if command -v realpath >/dev/null 2>&1; then
                rel_project_root=$(realpath "$PROJECT_ROOT" 2>/dev/null)
            else
                rel_project_root=$(cd "$PROJECT_ROOT" && pwd)
            fi
            error "Nem .env nem .env.template encontrados na raiz do projeto: ${rel_project_root}"
            echo "Por favor, crie um ficheiro .env na raiz do projeto com as configurações necessárias."
            exit 1
        fi
    fi

    # Debug: mostrar caminho absoluto do .env
    log "Caminho absoluto do .env: $(cd "$PROJECT_ROOT" && pwd)/.env"

    # Copiar .env para o diretório dos scrapers se necessário
    if [ ! -f "$SCRAPERS_DIR/.env" ] || ! cmp -s "$PROJECT_ROOT/.env" "$SCRAPERS_DIR/.env"; then
        log "A copiar .env da raiz do projeto para o diretório dos scrapers..."
        cp "$PROJECT_ROOT/.env" "$SCRAPERS_DIR/.env"
        success "Ficheiro .env copiado para o diretório dos scrapers"
    fi
    
    # Verificar se as variáveis essenciais estão definidas
    local missing_vars=()
    
    # Carregar o .env para verificação
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
    
    # Verificar variáveis da base de dados
    [ -z "$DB_USER" ] && missing_vars+=("DB_USER")
    [ -z "$DB_PASSWORD" ] && missing_vars+=("DB_PASSWORD")
    [ -z "$DB_HOST" ] && missing_vars+=("DB_HOST")
    
    # Verificar variáveis GCS
    [ -z "$GCS_PROJECT_ID" ] && missing_vars+=("GCS_PROJECT_ID")
    [ -z "$GCS_BUCKET_NAME" ] && missing_vars+=("GCS_BUCKET_NAME")
    
    if [ ${#missing_vars[@]} -gt 0 ]; then
        error "Variáveis de ambiente obrigatórias em falta no ficheiro .env:"
        for var in "${missing_vars[@]}"; do
            echo "  - $var"
        done
        echo ""
        echo "Por favor, edite $PROJECT_ROOT/.env e preencha estas variáveis."
        exit 1
    fi
    
    success "Configuração do ficheiro .env validada"
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
        echo "Isto normalmente significa que o Docker Desktop não está em execução."
        echo ""
        echo "Para corrigir isto:"
        echo "   1. Abra a aplicação Docker Desktop"
        echo "   2. Aguarde que o ícone da baleia do Docker apareça na barra de menu"
        echo "   3. Certifique-se que o Docker Desktop mostra 'Engine running'"
        echo "   4. Tente executar o script novamente"
        echo ""
        echo "Resolução de problemas alternativa:"
        echo "   • Verifique se o Docker funciona com sudo: sudo docker ps"
        echo "   • Adicione o seu utilizador ao grupo docker: sudo usermod -aG docker \$USER"
        echo "   • Reinicie o Docker Desktop se já estiver aberto"
        echo ""
        echo "Execute './run_local.sh docker-check' para diagnósticos detalhados"
        exit 1
    fi
    
    success "Docker está disponível e em execução"
    
    # Verificar e configurar .env
    setup_env_file
    
    # Parar completamente quaisquer containers existentes
    stop_docker_containers
    
    # Aguardar um momento para a limpeza terminar
    sleep 3
    
    # Verificar se o ficheiro .env existe no diretório dos scrapers
    if [ ! -f ".env" ]; then
        error "Ficheiro .env não encontrado no diretório dos scrapers"
        exit 1
    fi
    
    # Mostrar algumas variáveis de ambiente (sem mostrar passwords)
    log "Variáveis de ambiente carregadas:"
    echo "  DB_HOST: $(grep '^DB_HOST=' .env | cut -d'=' -f2)"
    echo "  DB_USER: $(grep '^DB_USER=' .env | cut -d'=' -f2)"
    echo "  GCS_PROJECT_ID: $(grep '^GCS_PROJECT_ID=' .env | cut -d'=' -f2)"
    echo "  GCS_BUCKET_NAME: $(grep '^GCS_BUCKET_NAME=' .env | cut -d'=' -f2)"
    
    # Construir e iniciar os containers
    log "A construir e iniciar containers de scrapers..."
    
    # Usar docker-compose com --env-file explícito para garantir que as variáveis são carregadas
    if command -v docker-compose >/dev/null 2>&1; then
        docker-compose --env-file .env up --build -d
    else
        docker compose --env-file .env up --build -d
    fi
    
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

# Função para parar containers Docker completamente
stop_docker_containers() {
    log "A parar containers Docker completamente..."
    
    cd "$SCRAPERS_DIR"
    
    # Verificar se o Docker está acessível antes de tentar parar containers
    if ! docker ps >/dev/null 2>&1; then
        warning "Docker não está acessível - a saltar limpeza de containers"
        kill_port $AIRFLOW_PORT
        return 0
    fi
    
    # Determinar comando docker-compose
    local compose_cmd="docker-compose"
    if ! command -v docker-compose >/dev/null 2>&1; then
        if docker compose version >/dev/null 2>&1; then
            compose_cmd="docker compose"
        fi
    fi
    
    # Parar e remover containers com timeout, usando --env-file se existir
    log "A parar containers..."
    if [ -f ".env" ]; then
        $compose_cmd --env-file .env stop --timeout 30 2>/dev/null || true
        log "A remover containers e redes..."
        $compose_cmd --env-file .env down --remove-orphans --volumes --timeout 10 2>/dev/null || true
    else
        $compose_cmd stop --timeout 30 2>/dev/null || true
        log "A remover containers e redes..."
        $compose_cmd down --remove-orphans --volumes --timeout 10 2>/dev/null || true
    fi
    
    # Limpar containers restantes relacionados com o projeto
    log "A limpar containers SIMPREDE restantes..."
    docker ps -a --filter "name=simprede" --format "{{.ID}}" | xargs -r docker rm -f 2>/dev/null || true
    docker ps -a --filter "name=airflow" --format "{{.ID}}" | xargs -r docker rm -f 2>/dev/null || true
    
    # Remover volumes pendentes deste projeto
    log "A limpar volumes do projeto..."
    docker volume ls --filter "name=simprede" --format "{{.Name}}" | xargs -r docker volume rm 2>/dev/null || true
    
    # Terminar processos que ainda usem a porta do Airflow
    kill_port $AIRFLOW_PORT
    
    success "containers Docker parados e limpos"
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
    
    # Parar containers de scrapers completamente
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
        echo -e "${GREEN}[ACTIVO]${NC} Dashboard: Em execução em http://localhost:$DASHBOARD_PORT"
    else
        echo -e "${RED}[INACTIVO]${NC} Dashboard: Não está em execução"
    fi
    
    # Verificar Airflow
    if check_port $AIRFLOW_PORT; then
        echo -e "${GREEN}[ACTIVO]${NC} Airflow: Em execução em http://localhost:$AIRFLOW_PORT"
        
        # Obter credenciais se o Airflow estiver em execução
        get_airflow_credentials
        echo ""
        echo "=== Credenciais do Airflow ==="
        echo -e "${GREEN}Utilizador:${NC} ${AIRFLOW_USERNAME:-admin}"
        echo -e "${GREEN}Password:${NC} ${AIRFLOW_PASSWORD:-<verificar logs>}"
    else
        echo -e "${RED}[INACTIVO]${NC} Airflow: Não está em execução"
    fi
    
    # Verificar containers Docker
    cd "$SCRAPERS_DIR"
    echo ""
    echo "=== containers Docker ==="
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
    clean           Parar e limpar containers e volumes Docker
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
    $0 clean                    # Limpar containers/volumes Docker
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

# Função para iniciar o ambiente
start_environment() {
    log "A iniciar ambiente de desenvolvimento local SIMPREDE..."
    setup_env_file
    setup_dashboard
    start_scrapers
    start_dashboard
    echo ""
    success "Todos os serviços iniciados com sucesso!"
    echo ""
    echo -e "${GREEN}Serviços SIMPREDE:${NC}"
    echo -e "   Dashboard:  http://localhost:$DASHBOARD_PORT"
    echo -e "   Airflow:    http://localhost:$AIRFLOW_PORT"
    echo ""
    echo -e "${GREEN}Credenciais do Airflow:${NC}"
    echo -e "   Utilizador: ${AIRFLOW_USERNAME:-admin}"
    echo -e "   Password: ${AIRFLOW_PASSWORD:-<verificar logs>}"
    echo ""
    echo -e "${YELLOW}Comandos úteis:${NC}"
    echo -e "   Verificar estado:  $0 status"
    echo -e "   Ver logs:          $0 logs [dashboard|scrapers]"
    echo -e "   Parar tudo:        $0 stop"
}

# Lógica principal do script
case ${1:-start} in
    "start")
        start_environment
        ;;
    "stop")
        stop_services
        ;;
    "clean")
        log "A limpar containers e volumes Docker..."
        stop_docker_containers
        success "Limpeza Docker concluída"
        ;;
    "docker-check"|"check-docker")
        log "A executar diagnósticos Docker..."
        echo ""
        echo "=== Diagnósticos Docker ==="
        echo ""
        echo "Para instalar o Docker e o Docker Compose, siga as instruções oficiais em:"
        echo "    https://docs.docker.com/compose/install/"
        echo ""
        echo "Esta página contém orientações detalhadas para Linux, MacOS e Windows."
        echo "Após instalar, reinicie o terminal e execute novamente este script."
        echo ""
        # Verificar se o comando docker existe
        if command -v docker >/dev/null 2>&1; then
            echo -e "${GREEN}[OK]${NC} Comando Docker encontrado: $(which docker)"
        else
            echo -e "${RED}[ERRO]${NC} Comando Docker não encontrado"
            exit 1
        fi
        
        # Verificar versão do Docker
        echo -n "Versão do Docker: "
        if docker --version 2>/dev/null; then
            echo -e "${GREEN}[OK]${NC} Versão do Docker acessível"
        else
            echo -e "${RED}[ERRO]${NC} Não é possível obter a versão do Docker"
        fi
        
        # Verificar ligação ao daemon Docker
        echo -n "Daemon Docker: "
        if docker info >/dev/null 2>&1; then
            echo -e "${GREEN}[OK]${NC} Daemon Docker acessível"
        else
            echo -e "${RED}[ERRO]${NC} Daemon Docker não acessível"
            echo "  Tente: sudo docker info"
            echo "  Ou verifique se o Docker Desktop está em execução"
        fi
        
        # Verificar se conseguimos listar containers
        echo -n "Listagem de containers: "
        if docker ps >/dev/null 2>&1; then
            echo -e "${GREEN}[OK]${NC} Consegue listar containers"
            echo "containers atuais:"
            docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || echo "Nenhum"
        else
            echo -e "${RED}[ERRO]${NC} Não consegue listar containers"
        fi
        
        # Verificar docker-compose
        echo -n "Docker Compose: "
        if command -v docker-compose >/dev/null 2>&1; then
            echo -e "${GREEN}[OK]${NC} docker-compose encontrado: $(docker-compose --version)"
        elif docker compose version >/dev/null 2>&1; then
            echo -e "${GREEN}[OK]${NC} plugin docker compose encontrado: $(docker compose version)"
        else
            echo -e "${RED}[ERRO]${NC} Nem docker-compose nem docker compose encontrados"
        fi
        
        # Verificar ficheiro .env
        echo -n "Ficheiro .env: "
        if [ -f "$PROJECT_ROOT/.env" ]; then
            echo -e "${GREEN}[OK]${NC} Encontrado em $PROJECT_ROOT/.env"
            # Mostrar algumas variáveis (sem passwords)
            echo "Variáveis definidas:"
            grep -E '^(DB_HOST|DB_USER|GCS_PROJECT_ID|GCS_BUCKET_NAME)=' "$PROJECT_ROOT/.env" | sed 's/^/  /' || echo "  Nenhuma variável essencial encontrada"
        else
            echo -e "${RED}[ERRO]${NC} Não encontrado em $PROJECT_ROOT/.env"
        fi
        
        echo ""
        ;;
    "restart")
        log "A reiniciar todos os serviços..."
        stop_services
        sleep 3
        setup_env_file
        setup_dashboard
        start_scrapers
        start_dashboard
        success "Todos os serviços reiniciados com sucesso!"
        echo ""
        echo -e "${GREEN}Credenciais do Airflow:${NC}"
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
            echo -e "${GREEN}Credenciais do Airflow:${NC}"
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
        log "A configurar apenas o ambiente do dashboard..."
        setup_env_file
        setup_dashboard
        success "Ambiente do dashboard configurado com sucesso!"
        echo ""
        echo "Para iniciar o dashboard execute: $0 dashboard"
        ;;
    "dashboard")
        setup_env_file
        setup_dashboard
        start_dashboard
        success "Dashboard iniciado em http://localhost:$DASHBOARD_PORT"
        ;;
    "scrapers")
        setup_env_file
        start_scrapers
        success "Scrapers iniciados em http://localhost:$AIRFLOW_PORT"
        echo ""
        echo -e "${GREEN}Credenciais do Airflow:${NC}"
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

# Fim do script