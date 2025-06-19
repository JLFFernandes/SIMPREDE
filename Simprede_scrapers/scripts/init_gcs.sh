#!/bin/bash
# GCS Setup Initialization Script for SIMPREDE Airflow
# This script automatically configures Google Cloud Storage integration

set -e

echo "🚀 Inicializando configuração do Google Cloud Storage..."

# Define paths
CONFIG_DIR="/opt/airflow/config"
GCS_CONFIG_FILE="/opt/airflow/scripts/google_scraper/config/gcs_config.json"
GCS_CREDENTIALS_FILE="$CONFIG_DIR/gcs-credentials.json"
GCS_CREDENTIALS_TEMPLATE="$CONFIG_DIR/gcs-credentials-template.json"

# Create config directory if it doesn't exist
mkdir -p "$CONFIG_DIR"

# Set default environment variables if not provided
export GCS_PROJECT_ID="${GCS_PROJECT_ID:-simprede}"
export GCS_BUCKET_NAME="${GCS_BUCKET_NAME:-simprede-data-pipeline}"
export GCS_LOCATION="${GCS_LOCATION:-EUROPE-WEST1}"

echo "📋 Configuração GCS:"
echo "  - Project ID: $GCS_PROJECT_ID"
echo "  - Bucket Name: $GCS_BUCKET_NAME"
echo "  - Location: $GCS_LOCATION"
echo "  - Credentials: $GCS_CREDENTIALS_FILE"

# Update GCS config file with environment variables
if [ -f "$GCS_CONFIG_FILE" ]; then
    echo "🔧 Atualizando ficheiro de configuração GCS..."
    
    # Create temporary config with environment variables
    cat > "$GCS_CONFIG_FILE.tmp" << EOF
{
  "project_id": "$GCS_PROJECT_ID",
  "bucket_name": "$GCS_BUCKET_NAME",
  "credentials_path": "$GCS_CREDENTIALS_FILE",
  "location": "$GCS_LOCATION",
  "description": "Configuração de Lago de dados do Simprede no Google Cloud Storage"
}
EOF
    
    # Replace the original file
    mv "$GCS_CONFIG_FILE.tmp" "$GCS_CONFIG_FILE"
    echo "✅ Ficheiro de configuração atualizado: $GCS_CONFIG_FILE"
else
    echo "⚠️ Ficheiro de configuração GCS não encontrado: $GCS_CONFIG_FILE"
fi

# Check for credentials file
if [ -f "$GCS_CREDENTIALS_FILE" ]; then
    echo "✅ Ficheiro de credenciais GCS encontrado: $GCS_CREDENTIALS_FILE"
    
    # Validate JSON format
    if python3 -c "import json; json.load(open('$GCS_CREDENTIALS_FILE'))" 2>/dev/null; then
        echo "✅ Ficheiro de credenciais é um JSON válido"
        
        # Extract project ID from credentials and verify it matches
        CRED_PROJECT_ID=$(python3 -c "import json; data=json.load(open('$GCS_CREDENTIALS_FILE')); print(data.get('project_id', ''))" 2>/dev/null || echo "")
        if [ -n "$CRED_PROJECT_ID" ] && [ "$CRED_PROJECT_ID" != "$GCS_PROJECT_ID" ]; then
            echo "⚠️ AVISO: Project ID nas credenciais ($CRED_PROJECT_ID) difere do configurado ($GCS_PROJECT_ID)"
        fi
    else
        echo "❌ ERRO: Ficheiro de credenciais não é um JSON válido"
        exit 1
    fi
    
    # Set up environment variable for Google Cloud SDK
    export GOOGLE_APPLICATION_CREDENTIALS="$GCS_CREDENTIALS_FILE"
    echo "✅ GOOGLE_APPLICATION_CREDENTIALS definido"
    
elif [ -f "$GCS_CREDENTIALS_TEMPLATE" ]; then
    echo "📝 Encontrado template de credenciais: $GCS_CREDENTIALS_TEMPLATE"
    echo "ℹ️ Copie o seu ficheiro de credenciais para: $GCS_CREDENTIALS_FILE"
else
    echo "⚠️ Nenhum ficheiro de credenciais encontrado"
    echo "ℹ️ Para configurar as credenciais:"
    echo "   1. Crie uma conta de serviço no Google Cloud Console"
    echo "   2. Baixe o ficheiro JSON de credenciais"
    echo "   3. Copie-o para: $GCS_CREDENTIALS_FILE"
    echo "   4. Ou defina GOOGLE_APPLICATION_CREDENTIALS no ambiente"
    
    # Create template file for user guidance
    cat > "$GCS_CREDENTIALS_TEMPLATE" << 'EOF'
{
  "type": "service_account",
  "project_id": "your-project-id",
  "private_key_id": "your-private-key-id",
  "private_key": "-----BEGIN PRIVATE KEY-----\nYOUR_PRIVATE_KEY_HERE\n-----END PRIVATE KEY-----\n",
  "client_email": "your-service-account@your-project.iam.gserviceaccount.com",
  "client_id": "your-client-id",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://token.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/your-service-account%40your-project.iam.gserviceaccount.com"
}
EOF
    echo "📝 Criado template de credenciais: $GCS_CREDENTIALS_TEMPLATE"
fi

# Set environment variables for the current session and future sessions
echo "🔧 Configurando variáveis de ambiente..."

# Create environment file for Airflow
ENV_FILE="$CONFIG_DIR/gcs_env.sh"
cat > "$ENV_FILE" << EOF
#!/bin/bash
# GCS Environment Variables for SIMPREDE
export GCS_PROJECT_ID="$GCS_PROJECT_ID"
export GCS_BUCKET_NAME="$GCS_BUCKET_NAME"
export GCS_LOCATION="$GCS_LOCATION"
export GOOGLE_APPLICATION_CREDENTIALS="$GCS_CREDENTIALS_FILE"
EOF

chmod +x "$ENV_FILE"
echo "✅ Ficheiro de ambiente criado: $ENV_FILE"

# Test GCS setup if credentials are available
if [ -f "$GCS_CREDENTIALS_FILE" ]; then
    echo "🧪 Testando configuração GCS..."
    
    # Test using Python
    python3 -c "
import os
import sys
sys.path.insert(0, '/opt/airflow/scripts/google_scraper/exportador_gcs')

try:
    from export_to_gcs_airflow import get_gcs_config
    config = get_gcs_config()
    print('✅ Configuração GCS carregada com sucesso')
    print(f'  - Project: {config[\"project_id\"]}')
    print(f'  - Bucket: {config[\"bucket_name\"]}')
except Exception as e:
    print(f'⚠️ Erro ao testar configuração: {e}')
    sys.exit(0)  # Don't fail the setup, just warn
" || echo "⚠️ Teste de configuração falhou (bibliotecas GCS podem não estar instaladas ainda)"
else
    echo "ℹ️ Teste GCS ignorado - sem credenciais disponíveis"
fi

# Create directories for GCS export logs
mkdir -p "/opt/airflow/logs/gcs_export"
chmod 755 "/opt/airflow/logs/gcs_export"

echo "✅ Inicialização GCS concluída!"
echo ""
echo "🎯 Próximos passos:"
if [ ! -f "$GCS_CREDENTIALS_FILE" ]; then
    echo "   1. Copie o seu ficheiro de credenciais para: $GCS_CREDENTIALS_FILE"
    echo "   2. Reinicie o contentor Airflow"
    echo "   3. Execute o DAG 'pipeline_scraper_google'"
else
    echo "   1. Execute o DAG 'pipeline_scraper_google'"
    echo "   2. Verifique os logs da tarefa 'exportar_para_gcs'"
    echo "   3. Confirme que os ficheiros foram carregados no bucket: gs://$GCS_BUCKET_NAME"
fi
echo ""

# Source environment variables if .env file exists
# Check project root first, then container locations
ENV_LOCATIONS=(
    "/opt/airflow/../.env"
    "/opt/airflow/.env"
    "/.env"
    "/tmp/.env"
)

for env_file in "${ENV_LOCATIONS[@]}"; do
    if [ -f "$env_file" ]; then
        log "Loading environment from: $env_file"
        set -a
        source "$env_file"
        set +a
        break
    fi
done
