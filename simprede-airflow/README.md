# SIMPREDE Airflow Container

This directory contains the necessary files to run Apache Airflow in a containerized environment for the SIMPREDE project. The setup includes the Google News Scraper pipeline and related components.

## Prerequisites

- Docker and Docker Compose installed on your system
- At least 4GB of RAM allocated to Docker
- At least 2 CPU cores allocated to Docker
- At least 10GB of free disk space

## Quick Start

### 🚀 One-Command Setup
```bash
./start_airflow.sh
```
This streamlined script handles everything:
- ✅ Checks Docker prerequisites
- ✅ Sets up GCS configuration with defaults
- ✅ Creates `.env` file with optimal settings
- ✅ Builds Docker images with GCS support
- ✅ Starts all containers
- ✅ Displays admin credentials and GCS status
- ✅ Shows next steps for GCS activation

### ☁️ Enable GCS Export (Optional)
To activate automatic export to Google Cloud Storage:
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a service account with **Storage Admin** role
3. Download the JSON key file and save as: `./config/gcs-credentials.json`
4. Restart: `./restart_airflow.sh` (shows updated GCS status)

### ⚡ Daily Operations
```bash
# Restart with GCS status check
./restart_airflow.sh

# Stop all containers
./stop_airflow.sh

# Troubleshooting validation
./validate_setup.sh

# Quick GCS validation
docker compose exec airflow-standalone python3 /opt/airflow/scripts/google_scraper/exportador_gcs/validate_gcs_setup.py
```

### 🔑 Default Credentials
- **Username**: `admin`
- **Password**: `simprede`
- **Web UI**: http://localhost:8080

## Directory Structure

- `dags/`: Contains all the Airflow DAGs, including the Google scraper
- `logs/`: Directory for Airflow logs (persisted on host)
- `plugins/`: Directory for Airflow plugins
- `data/`: **Main data directory (persisted on host)**
  - `data/raw/`: Raw scraped data from Google News
  - `data/structured/`: Processed articles with extracted information
  - `data/processed/`: Filtered articles ready for export
- `scripts/`: Contains the Google scraper scripts (mounted as volume)
- `config/`: Configuration files for scraper

## Data Persistence

All extracted data is automatically saved to your host machine in the `./data/` directory:

```
./data/
├── raw/           # Raw Google News articles (CSV format)
│   └── YYYY/MM/DD/  # Organized by date
├── structured/    # Processed articles with disaster info
│   └── YYYY/MM/DD/  # Organized by date  
└── processed/     # Final filtered articles
    └── YYYY/MM/DD/  # Organized by date
```

### 📁 Data Access

- **Location**: All data is saved in `./data/` relative to this directory
- **Format**: CSV files with timestamped names
- **Organization**: Files are organized by year/month/day for easy access
- **Persistence**: Data survives container restarts and rebuilds

### 🔍 Example File Locations

After running the scraper, you'll find files like:
- `./data/raw/2024/01/15/intermediate_google_news_20240115.csv`
- `./data/structured/2024/01/15/artigos_google_municipios_pt_2024-01-15.csv`
- `./data/processed/2024/01/15/artigos_vitimas_filtrados_2024-01-15.csv`

## Configuration

The environment variables can be configured in the `.env` file. The default configuration includes:

- **Airflow credentials**: Username `admin`, Password auto-gerado
- **PostgreSQL database**: Configuração interna do Airflow
- **Supabase database**: Configuração para exportação de dados
- Other settings for the Google scraper and additional components

### ⚠️ Proteção do Ficheiro .env

**IMPORTANTE**: O ficheiro `.env` contém credenciais sensíveis da base de dados Supabase. Os scripts de arranque foram otimizados para preservar estas credenciais:

- `start_airflow.sh` apenas adiciona/atualiza `AIRFLOW_UID` sem eliminar outras variáveis
- `restart_airflow.sh` verifica a integridade das credenciais antes do reinício
- Nunca edite manualmente as linhas das credenciais da base de dados

### Verificação das Credenciais

Para verificar se as credenciais estão intactas:
```bash
./get_admin_password.sh
```

Se as credenciais da base de dados estiverem em falta, restaure-as no ficheiro `.env`:
```bash
DB_HOST=aws-0-eu-west-3.pooler.supabase.com
DB_PORT=6543
DB_NAME=postgres
DB_USER=postgres.kyrfsylobmsdjlrrpful
DB_PASSWORD=HXU3tLVVXRa1jtjo
DB_SSLMODE=require
DB_SCHEMA=google_scraper
```

## Primeiros Passos

1. Construir e arrancar os contentores:

   ```bash
   ./start_airflow.sh
   ```

2. Aceder à interface web do Airflow:
   
   Abra o navegador e vá para `http://localhost:8080`
   
   Faça login com as credenciais mostradas pelo script de arranque

3. Ativar os DAGs que pretende executar

## Paragem dos Contentores

Para parar todos os contentores:

```bash
docker-compose down
```

Para parar todos os contentores e remover volumes (isto eliminará todos os dados na base de dados PostgreSQL):

```bash
docker-compose down -v
```

## Resolução de Problemas

- Se encontrar problemas de permissões, certifique-se de que `AIRFLOW_UID` no ficheiro `.env` está definido corretamente
- **Credenciais em Falta**: Se as credenciais da base de dados Supabase estiverem em falta, restaure-as manualmente no `.env`
- **Dependências de Dados**: Algumas tarefas podem ignorar graciosamente quando não há dados de entrada disponíveis
- Para outros problemas, verifique os logs no diretório `logs/` ou através da interface web do Airflow

## Notas Adicionais

- **Persistência de Dados**: Todos os dados extraídos são automaticamente guardados no diretório `./data/` na máquina host
- **Proteção do .env**: Os scripts preservam automaticamente as credenciais da base de dados no ficheiro `.env`
- O Google scraper está configurado para executar em modo headless por padrão. Isto pode ser alterado no ficheiro `.env`
- Para atualizar as dependências Python, modifique o ficheiro `requirements.txt` e reconstrua o contentor
- **Localização dos Dados**: Verifique `./data/raw/`, `./data/structured/`, e `./data/processed/` para os ficheiros extraídos
