# SIMPREDE - Sistema Integrado de Monitorização e Prevenção de Desastres

Um sistema completo de pipeline de dados utilizando Apache Airflow para extração, processamento e análise de notícias relacionadas com eventos de desastres naturais.

## Índice

- [Visão Geral](#visão-geral)
- [Pré-requisitos](#pré-requisitos)
- [Instalação e Configuração](#instalação-e-configuração)
- [Configuração de Credenciais](#configuração-de-credenciais)
- [Execução do Sistema](#execução-do-sistema)
- [Acesso às Interfaces](#acesso-às-interfaces)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Resolução de Problemas](#resolução-de-problemas)
- [Comandos Úteis](#comandos-úteis)

## Visão Geral

O SIMPREDE é um sistema automatizado que:

- **Web Scraping**: Extrai notícias do Google News sobre eventos de desastres
- **Processamento**: Analisa e normaliza dados utilizando processamento de linguagem natural
- **Geocodificação**: Identifica e valida localizações geográficas nos artigos
- **Armazenamento**: Exporta dados para Google Cloud Storage
- **Dashboard**: Interface visual para análise de dados via Streamlit
- **Base de Dados**: Integração com PostgreSQL/Supabase para persistência

### Características Principais

- Interface web Apache Airflow para gestão de pipelines
- Dashboard interativo com Streamlit
- Execução automatizada via Docker
- Suporte para Google Cloud Storage
- Processamento de linguagem natural para português
- Sistema de geocodificação para Portugal e Moçambique
- Monitorização e logs detalhados

## Pré-requisitos

### Software Necessário

- **Docker Desktop** (versão 4.0 ou superior) - [Download aqui](https://www.docker.com/products/docker-desktop/)
- **Git** para clonar o repositório
- **Python 3.8+** (para o dashboard local)
- **Conta Google Cloud Platform** (opcional, para GCS)

### Requisitos do Sistema

- **macOS**: 10.15 ou superior / **Linux**: Ubuntu 18.04+ / **Windows**: 10 Pro ou superior
- **RAM**: Mínimo 8GB, recomendado 16GB
- **Espaço em Disco**: Mínimo 10GB livres
- **Portas**: 8080 (Airflow), 8501 (Dashboard), 5432 (PostgreSQL) disponíveis

## Instalação e Configuração

### 1. Clonar o Repositório

```bash
git clone https://github.com/seu-usuario/SIMPREDE.git
cd SIMPREDE
```

### 2. Configuração Inicial Automática

Execute o script automatizado que configura todo o ambiente:

```bash
chmod +x run_local.sh
./run_local.sh start
```

**OU** configure manualmente seguindo os passos abaixo:

### 3. Configuração Manual (Alternativa)

#### 3.1 Configurar Ficheiro .env

```bash
# Copiar o template
cp .env.template .env

# Editar o ficheiro .env com as suas configurações
nano .env
```

#### 3.2 Iniciar apenas os Scrapers

```bash
cd Simprede_scrapers
chmod +x start_airflow.sh
./start_airflow.sh
```

#### 3.3 Configurar e Iniciar o Dashboard

```bash
cd Simprede_dashboard
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

## Configuração de Credenciais

### Localização dos Ficheiros de Configuração

```
SIMPREDE/
├── .env                          ← FICHEIRO PRINCIPAL (raiz do projeto) **OBRIGATÓRIO**
├── .env.template                 ← Template de exemplo
├── Simprede_scrapers/
│   └── config/
│       └── gcs-credentials.json  ← Credenciais GCS (opcional) **COLOCAR AQUI**
└── Simprede_dashboard/
    └── .streamlit/
        └── secrets.toml          ← Credenciais do dashboard
```

**FICHEIROS OBRIGATÓRIOS A ADICIONAR:**

1. **Ficheiro `.env`** → Deve estar na **raiz do projeto** (`/SIMPREDE/.env`)
2. **Ficheiro `gcs-credentials.json`** → Deve estar em `Simprede_scrapers/config/gcs-credentials.json` (apenas se usar Service Account)

### Configuração do Ficheiro .env

**IMPORTANTE**: O ficheiro `.env` deve estar na **raiz do projeto** (`/SIMPREDE/.env`), não nas subpastas.

Edite o ficheiro `.env` com as suas credenciais:

```bash
# === CONFIGURAÇÃO OBRIGATÓRIA ===

# Configuração da Base de Dados (Supabase/PostgreSQL)
DB_USER=postgres.kyrfsylobmsdjlrrpful
DB_PASSWORD=HXU3tLVVXRa1jtjo
DB_HOST=aws-0-eu-west-3.pooler.supabase.com
DB_PORT=6543
DB_NAME=postgres
DB_SCHEMA=google_scraper

# Credenciais API Supabase
SUPABASE_URL=https://kyrfsylobmsdjlrrpful.supabase.co
SUPABASE_ANON_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...

# === CONFIGURAÇÃO OPCIONAL ===

# Google Cloud Storage (usando credenciais padrão do utilizador)
GCS_PROJECT_ID=simprede-461309
GCS_BUCKET_NAME=simprede-data-pipeline
GCS_LOCATION=EUROPE-WEST1
GCS_DEBUG=false

# Configuração Airflow
AIRFLOW_UID=501
```

### Configuração do Google Cloud Storage

#### Opção 1: Credenciais de Utilizador (Recomendado para Desenvolvimento)

1. **Instalar Google Cloud SDK**:
   ```bash
   # macOS (via Homebrew)
   brew install google-cloud-sdk
   
   # Linux/Windows - seguir: https://cloud.google.com/sdk/docs/install
   ```

2. **Autenticar com a sua conta Google**:
   ```bash
   gcloud auth application-default login
   gcloud config set project simprede-461309
   ```

3. **Verificar autenticação**:
   ```bash
   gcloud auth list
   ```

#### Opção 2: Service Account (Recomendado para Produção)

1. **Ir ao Google Cloud Console**: https://console.cloud.google.com/

2. **Criar Service Account**:
   - IAM & Admin → Service Accounts
   - "Criar Service Account"
   - Nome: `simprede-storage-admin`
   - Atribuir função: "Storage Admin"

3. **Gerar e descarregar credenciais**:
   - Clicar na Service Account criada
   - Separador "Chaves" → "Adicionar Chave" → "Criar nova chave"
   - Formato: JSON
   - Descarregar o ficheiro

4. **Colocar o ficheiro de credenciais no local correcto**:
   ```bash
   # Criar o diretório se não existir
   mkdir -p Simprede_scrapers/config
   
   # Mover o ficheiro descarregado para a localização correcta
   mv ~/Downloads/simprede-xxxxx.json Simprede_scrapers/config/gcs-credentials.json
   
   # IMPORTANTE: O ficheiro deve estar exactamente em:
   # Simprede_scrapers/config/gcs-credentials.json
   ```

5. **Atualizar .env** (se usar Service Account):
   ```bash
   # Adicionar ao ficheiro .env na raiz do projeto:
   GOOGLE_APPLICATION_CREDENTIALS=./config/gcs-credentials.json
   ```

**RESUMO DOS FICHEIROS NECESSÁRIOS:**

- **`.env`** → `/SIMPREDE/.env` (raiz do projeto)
- **`gcs-credentials.json`** → `/SIMPREDE/Simprede_scrapers/config/gcs-credentials.json` (apenas para Service Account)

### Configuração do Dashboard

Se executar o dashboard separadamente, crie o ficheiro de configuração:

```bash
mkdir -p Simprede_dashboard/.streamlit
cat > Simprede_dashboard/.streamlit/secrets.toml << EOF
[connections.supabase]
type = "supabase"
url = "https://kyrfsylobmsdjlrrpful.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."

[database]
host = "aws-0-eu-west-3.pooler.supabase.com"
port = 6543
database = "postgres"
username = "postgres.kyrfsylobmsdjlrrpful"
password = "HXU3tLVVXRa1jtjo"
EOF
```

## Execução do Sistema

### Método 1: Execução Completa (Recomendado)

Execute ambos os componentes (scrapers + dashboard) automaticamente:

```bash
# Na raiz do projeto
./run_local.sh start
```

### Método 2: Execução Individual

#### Iniciar apenas os Scrapers (Airflow):
```bash
./run_local.sh scrapers
```

#### Iniciar apenas o Dashboard:
```bash
./run_local.sh dashboard
```

### Comandos de Gestão

```bash
# Verificar estado dos serviços
./run_local.sh status

# Ver credenciais do Airflow
./run_local.sh credentials

# Parar todos os serviços
./run_local.sh stop

# Reiniciar tudo
./run_local.sh restart

# Ver logs
./run_local.sh logs dashboard    # Logs do dashboard
./run_local.sh logs scrapers     # Logs do Airflow

# Limpeza completa (remove containers e volumes)
./run_local.sh clean

# Diagnósticos Docker
./run_local.sh docker-check
```

## Acesso às Interfaces

### Apache Airflow (Scrapers e Pipelines)
- **URL**: http://localhost:8080
- **Utilizador**: `admin`
- **Password**: Gerada automaticamente
  - Ver com: `./run_local.sh credentials`
  - Ou verificar logs: `docker-compose logs airflow-standalone | grep -i password`

### Dashboard Streamlit
- **URL**: http://localhost:8501
- Sem autenticação necessária

### Verificação de Funcionalidade

1. **Testar ligação à base de dados**:
   ```bash
   # Dentro do container Airflow
   docker-compose -f Simprede_scrapers/docker-compose.yml exec airflow-standalone python3 -c "
   import psycopg2
   conn = psycopg2.connect(
       host='aws-0-eu-west-3.pooler.supabase.com',
       port=6543,
       user='postgres.kyrfsylobmsdjlrrpful',
       password='HXU3tLVVXRa1jtjo',
       database='postgres'
   )
   print('Ligação PostgreSQL bem-sucedida')
   "
   ```

2. **Testar configuração GCS**:
   ```bash
   cd Simprede_scrapers
   docker-compose exec airflow-standalone python3 /opt/airflow/scripts/google_scraper/exportador_gcs/validate_gcs_setup.py
   ```

## Estrutura do Projeto
## b4. Estrutura do Projeto

```
SIMPREDE/
├── instalar-wsl2-para-simprede.ps1      ← Instalação WSL2 (Windows)
├── README.md                            ← Este ficheiro
├── run_local.sh                         ← Script principal de execução local
│
├── Simprede_dashboard/                  ← Interface visual (Streamlit)
│   ├── app.py                           ← Aplicação principal
│   ├── dashboard.log                    ← Log da aplicação
│   ├── LEI.png                          ← Logo/Imagem
│   ├── requirements.txt                 ← Dependências Python
│   └── UAB.png                          ← Logo/Imagem
│
├── Simprede_data_base/
│   └── simprede_db.sql                  ← Script SQL da base de dados
│
└── Simprede_scrapers/                   ← Pipeline de dados (Airflow)
    ├── config/                          ← Configurações
    │   ├── airflow.cfg
    │   ├── gcs_env.sh
    │   └── gcs-credentials.json
    ├── dags/                            ← DAGs Airflow
    │   ├── daily_eventos_processing_dag.py
    │   ├── emdat_final_dag.py
    │   ├── eswd_final_dag.py
    │   ├── geomai_dag.py
    │   ├── google_scraper_dag.py
    │   └── pts_disaster_dag.py
    ├── docker-compose.yml               ← Configuração Docker
    ├── Dockerfile                       ← Imagem personalizada
    ├── include/                         ← Dados geográficos e outros
    ├── README.md                        ← Documentação do módulo
    ├── requirements.txt                 ← Dependências Python
    ├── reset_admin_user.sh              ← Script de reset do utilizador admin
    ├── scripts/                         ← Scripts de apoio e módulos
    │   ├── docker-entrypoint.sh
    │   ├── EM-DAT_final.py
    │   ├── ESWD_final.py
    │   ├── geomai_final.py
    │   ├── google_scraper/
    │   │   ├── config/
    │   │   ├── exportador_bd/
    │   │   ├── exportador_gcs/
    │   │   ├── extracao/
    │   │   ├── models/
    │   │   ├── processador/
    │   │   ├── scraping/
    │   │   ├── tests/
    │   │   └── utils/
    │   ├── init_gcs.sh
    │   └── pts_disaster_export.py
    ├── setup_gcs_complete.sh            ← Script de setup GCS
    ├── start_airflow.sh                 ← Script de arranque do Airflow
    └── stop_airflow.sh                  ← Script para parar o Airflow
```

**Notas:**
- Todos os scripts de processamento, scraping e apoio estão em `Simprede_scrapers/scripts/`.
- As DAGs em `Simprede_scrapers/dags/` controlam os pipelines de dados no Airflow.
- Scripts de setup, arranque e reset encontram-se na raiz

## Resolução de Problemas

### Problema: Docker não está acessível

```bash
# Verificar se o Docker Desktop está em execução
./run_local.sh docker-check

# Se necessário, reiniciar Docker Desktop
# macOS: Spotlight → "Docker Desktop" → Abrir
# Windows: Menu Iniciar → "Docker Desktop"
```

### Problema: Erro "Port already in use"

```bash
# Verificar que processos estão a usar as portas
lsof -i :8080  # Airflow
lsof -i :8501  # Dashboard

# Parar todos os serviços e limpar
./run_local.sh stop
./run_local.sh clean
```

### Problema: Credenciais da base de dados inválidas

1. Verificar se as credenciais no `.env` estão corretas
2. Testar ligação manual:
   ```bash
   psql -h aws-0-eu-west-3.pooler.supabase.com -p 6543 -U postgres.kyrfsylobmsdjlrrpful -d postgres
   ```

### Problema: Erro de permissões

```bash
# Corrigir permissões dos directórios
chmod -R 755 Simprede_scrapers/data
chmod -R 755 Simprede_scrapers/logs
chmod -R 755 Simprede_scrapers/config

# Se necessário, usar sudo (Linux)
sudo chown -R $(id -u):$(id -g) Simprede_scrapers/data
```

### Problema: GCS não funciona

1. **Verificar autenticação**:
   ```bash
   gcloud auth list
   gcloud auth application-default print-access-token
   ```

2. **Re-autenticar**:
   ```bash
   gcloud auth application-default login
   ```

3. **Verificar permissões do projeto**:
   - IAM & Admin → IAM
   - Verificar se tem role "Storage Admin" ou "Editor"

### Problema: DAGs não aparecem no Airflow

1. Verificar se os ficheiros estão na pasta correcta:
   ```bash
   ls -la Simprede_scrapers/dags/
   ```

2. Verificar logs por erros de sintaxe:
   ```bash
   ./run_local.sh logs scrapers | grep -i error
   ```

3. Refrescar DAGs no Airflow:
   - Interface web → Admin → "Refresh DAGs"

## Comandos Úteis

### Gestão de Docker

```bash
# Ver estado dos containers
docker ps

# Ver logs de um container específico
docker logs simprede_airflow_container

# Entrar no container Airflow
docker-compose -f Simprede_scrapers/docker-compose.yml exec airflow-standalone bash

# Reconstruir imagens do zero
docker-compose -f Simprede_scrapers/docker-compose.yml build --no-cache

# Limpar tudo (cuidado: remove dados!)
docker system prune -a --volumes
```

### Gestão de Dados

```bash
# Ver tamanho dos dados
du -sh Simprede_scrapers/data/*

# Fazer backup
tar -czf backup_$(date +%Y%m%d).tar.gz Simprede_scrapers/data Simprede_scrapers/config .env

# Limpar dados antigos (manter últimos 7 dias)
find Simprede_scrapers/data -type f -mtime +7 -delete
```

### Teste e Validação

```bash
# Testar providers Airflow instalados
cd Simprede_scrapers
docker-compose exec airflow-standalone python3 -c "
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
print('Providers instalados correctamente')
"

# Validar configuração completa
./run_local.sh status
```

## Fluxo de Trabalho Típico

1. **Configurar credenciais**: Editar `.env` na raiz do projeto
2. **Iniciar sistema**: `./run_local.sh start`
3. **Aceder ao Airflow**: http://localhost:8080 (usar credenciais mostradas)
4. **Activar DAGs**: Toggle dos DAGs na interface web
5. **Monitorizar execução**: Ver progresso e logs
6. **Verificar dados**: Pasta `Simprede_scrapers/data/`
7. **Aceder ao dashboard**: http://localhost:8501
8. **Parar sistema**: `./run_local.sh stop`

## Suporte e Documentação

### Logs Importantes

- **Airflow**: `./run_local.sh logs scrapers`
- **Dashboard**: `./run_local.sh logs dashboard`
- **Docker**: `docker-compose -f Simprede_scrapers/docker-compose.yml logs`

### Ficheiros de Configuração Críticos

- **`.env`** (raiz): Configuração principal
- **`docker-compose.yml`**: Orquestração de serviços
- **`requirements.txt`**: Dependências Python

### Para Reportar Problemas

1. Executar diagnósticos: `./run_local.sh docker-check`
2. Recolher logs: `./run_local.sh logs scrapers > debug.log`
3. Verificar configuração: `cat .env` (sem mostrar passwords)
4. Incluir versões: `docker --version`, `python3 --version`

---

**SIMPREDE** - Sistema Integrado de Monitorização e Prevenção de Desastres  
Desenvolvido para automatização de pipelines de dados de eventos de desastres naturais.

## Instalação no Windows

Para executar o SIMPREDE no Windows, recomendamos utilizar o Windows Subsystem for Linux 2 (WSL2), pois o sistema foi desenvolvido para ambientes Unix/Linux.

### Requisitos para Windows

- Windows 10 versão 1903 ou superior (build 18362 ou superior)
- Acesso de administrador ao seu computador
- Mínimo de 8GB de RAM recomendado
- 10GB de espaço livre em disco

### Instalação Automática com WSL2 (Recomendado)

1. **Download do Script de Instalação**:
   - Baixe o [script de instalação automática](scripts/instalar-wsl2-para-simprede.ps1)
   - Salve como `instalar-wsl2-para-simprede.ps1`

2. **Executar o Script como Administrador**:
   - Clique com o botão direito no PowerShell e selecione "Executar como administrador"
   - Navegue até a pasta onde salvou o script
   - Execute: 
     ```powershell
     Set-ExecutionPolicy Bypass -Scope Process -Force
     .\instalar-wsl2-para-simprede.ps1
     ```
   - Siga as instruções na tela

3. **Reiniciar o Computador**:
   - Quando solicitado, reinicie o computador para finalizar a instalação

4. **Finalizar a Configuração**:
   - Após reiniciar, abra o Ubuntu pelo menu Iniciar
   - Execute o script de configuração: `~/simprede_wsl_setup.sh`

### Instalação Manual no Windows

Se preferir realizar a instalação manualmente, siga estes passos:

#### 1. Instalar o WSL2
1. Abra o PowerShell como Administrador
2. Execute:
   ```powershell
   dism.exe /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart
   dism.exe /online /enable-feature /featurename:VirtualMachinePlatform /all /norestart
   ```
3. Reinicie o computador
4. Baixe e instale o [pacote de atualização do kernel do WSL2](https://wslstorestorage.blob.core.windows.net/wslblob/wsl_update_x64.msi)
5. Defina o WSL2 como versão padrão:
   ```powershell
   wsl --set-default-version 2
   ```

#### 2. Instalar o Ubuntu
1. Abra a Microsoft Store
2. Pesquise por "Ubuntu"
3. Selecione "Ubuntu" (sem versão específica) ou "Ubuntu 22.04 LTS"
4. Clique em "Obter" e aguarde a instalação
5. Inicie o Ubuntu e crie seu nome de usuário e senha

#### 3. Configurar o ambiente no Ubuntu
1. Atualize o sistema:
   ```bash
   sudo apt update && sudo apt upgrade -y
   ```
2. Instale as dependências:
   ```bash
   sudo apt install -y git python3-pip python3-venv build-essential libssl-dev libffi-dev python3-dev docker-compose
   ```

#### 4. Instalar o Docker Desktop
1. Baixe o [Docker Desktop para Windows](https://desktop.docker.com/win/main/amd64/Docker%20Desktop%20Installer.exe)
2. Execute o instalador
3. Durante a instalação, certifique-se de que a opção "Use WSL 2 instead of Hyper-V" está selecionada
4. Conclua a instalação e reinicie se solicitado

#### 5. Configurar o projeto SIMPREDE
1. Clone o repositório e configure conforme instruções gerais na seção anterior

### Executando o SIMPREDE no Windows

1. Inicie o Docker Desktop no Windows
2. Abra o Ubuntu pelo menu Iniciar ou execute `wsl` no PowerShell
3. Navegue até a pasta do projeto e execute conforme instruções gerais

### Solução de Problemas no Windows

#### Problema: Docker não está disponível no WSL2
1. Abra o Docker Desktop
2. Acesse Configurações > Resources > WSL Integration
3. Ative a integração para sua distribuição Ubuntu

#### Problema: Permissões de arquivo
Se encontrar problemas de permissão:
```bash
sudo chmod -R 755 ~/projects/SIMPREDE/Simprede_scrapers/data
sudo chmod -R 755 ~/projects/SIMPREDE/Simprede_scrapers/logs
sudo chmod -R 755 ~/projects/SIMPREDE/Simprede_scrapers/config
```

