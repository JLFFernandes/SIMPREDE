# SIMPREDE - Pipeline de Dados com Apache Airflow

Este repositório contém a implementação do pipeline de dados SIMPREDE utilizando Apache Airflow em ambiente Docker. O sistema inclui web scraping do Google News, processamento de dados e exportação para Google Cloud Storage.

## 📋 Índice

- [Visão Geral](#visão-geral)
- [Pré-requisitos](#pré-requisitos)
- [Instalação e Configuração](#instalação-e-configuração)
- [Configuração do Google Cloud Storage](#configuração-do-google-cloud-storage)
- [Configuração da Base de Dados](#configuração-da-base-de-dados)
- [Utilização](#utilização)
- [DAGs Disponíveis](#dags-disponíveis)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Monitorização e Logs](#monitorização-e-logs)
- [Resolução de Problemas](#resolução-de-problemas)
- [Comandos Úteis](#comandos-úteis)
- [Contribuição](#contribuição)

## 🎯 Visão Geral

O SIMPREDE é um sistema de pipeline de dados automatizado que:

- **Web Scraping**: Extrai notícias do Google News relacionadas com eventos de desastres
- **Processamento**: Analisa e normaliza dados de notícias
- **Geocodificação**: Identifica localizações geográficas nos artigos
- **Exportação**: Guarda dados processados no Google Cloud Storage
- **Base de Dados**: Integra com PostgreSQL/Supabase para armazenamento persistente

### Características Principais

- ✅ Interface web Apache Airflow para gestão de pipelines
- ✅ Execução automatizada via Docker
- ✅ Suporte para Google Cloud Storage
- ✅ Processamento de linguagem natural para português
- ✅ Sistema de geocodificação para Portugal e Moçambique
- ✅ Monitorização e logs detalhados

## 🛠 Pré-requisitos

### Software Necessário

- **Docker Desktop** (versão 4.0 ou superior)
- **Docker Compose** (incluído no Docker Desktop)
- **Git** para clonar o repositório
- **Conta Google Cloud Platform** (opcional, para GCS)

### Requisitos do Sistema

- **macOS**: 10.15 ou superior
- **RAM**: Mínimo 4GB, recomendado 8GB
- **Espaço em Disco**: Mínimo 5GB livres
- **Portas**: 8080 (Airflow) e 5432 (PostgreSQL) disponíveis

## 🚀 Instalação e Configuração

### 1. Clonar o Repositório

```bash
git clone <url-do-repositorio>
cd simprede-airflow
```

### 2. Configuração Inicial

Execute o script de arranque que configura automaticamente o ambiente:

```bash
cp .env.template .env
chmod +x start_airflow.sh
./start_airflow.sh
```

Este script irá:
- Verificar pré-requisitos do Docker
- Configurar permissões dos directórios
- Construir as imagens Docker
- Inicializar a base de dados Airflow
- Arrancar todos os serviços
- Apresentar as credenciais de administrador

### 3. Acesso à Interface Web

Após a inicialização:
- **URL**: http://localhost:8080
- **Utilizador**: `admin`
- **Password**: *(gerada automaticamente e apresentada no terminal)*

## ☁️ Configuração do Google Cloud Storage

### Opção 1: Credenciais de Utilizador (Recomendado para Desenvolvimento)

```bash
# Instalar Google Cloud SDK
# Efectuar login
gcloud auth application-default login

# Definir projeto
gcloud config set project simprede-461309
```

### Opção 2: Service Account (Recomendado para Produção)

1. **Criar Service Account no Google Cloud Console**:
   - Vá a https://console.cloud.google.com/
   - IAM & Admin → Service Accounts
   - Criar nova Service Account
   - Atribuir role "Storage Admin"

2. **Baixar Credenciais**:
   - Baixar o ficheiro JSON de credenciais
   - Guardar como `./config/gcs-credentials.json`

3. **Reiniciar Serviços**:
   ```bash
   ./restart_airflow.sh
   ```

### Verificar Configuração GCS

```bash
docker compose exec airflow-standalone python3 /opt/airflow/scripts/google_scraper/exportador_gcs/validate_gcs_setup.py
```

## 🗄 Configuração da Base de Dados

### Credenciais Supabase

O projeto está configurado para usar Supabase como base de dados PostgreSQL. As credenciais estão no ficheiro `.env`:

```bash
# Supabase/PostgreSQL Credenciais
DB_USER=postgres.kyrfsylobmsdjlrrpful
DB_PASSWORD=HXU3tLVVXRa1jtjo
DB_HOST=aws-0-eu-west-3.pooler.supabase.com
DB_PORT=6543
DB_NAME=postgres
DB_SCHEMA=google_scraper
```

### Testar Ligação à Base de Dados

```bash
docker compose exec airflow-standalone python3 -c "
from airflow.providers.postgres.hooks.postgres import PostgresHook
hook = PostgresHook(postgres_conn_id='postgres_default')
print('✅ Ligação PostgreSQL bem-sucedida')
"
```

## 📊 Utilização

### Arrancar o Sistema

```bash
./start_airflow.sh
```

### Parar o Sistema

```bash
./stop_airflow.sh
```

### Reiniciar o Sistema

```bash
./restart_airflow.sh
```

### Executar DAGs

1. Aceder à interface web em http://localhost:8080
2. Na página "DAGs", localizar o DAG desejado
3. Activar o DAG usando o toggle
4. Clicar em "Trigger DAG" para execução manual

## 📈 DAGs Disponíveis

### `daily_eventos_processing_dag`
- **Descrição**: Pipeline principal de processamento diário de eventos
- **Frequência**: Diário às 06:00
- **Tarefas**:
  - Scraping de notícias do Google News
  - Processamento e análise de conteúdo
  - Geocodificação de localizações
  - Exportação para GCS

### `pipeline_scraper_google`
- **Descrição**: Pipeline de web scraping específico do Google News
- **Frequência**: Manual ou programado
- **Tarefas**:
  - Extracção de notícias
  - Normalização de dados
  - Filtragem por relevância

### `debug_coordinates_dag`
- **Descrição**: DAG de debug para geocodificação
- **Utilização**: Testes e validação de coordenadas

## 📁 Estrutura do Projeto

```
simprede-airflow/
├── Dockerfile                     # Configuração Docker Airflow
├── docker-compose.yml            # Orquestração de serviços
├── requirements.txt               # Dependências Python
├── .env.template                # Exemplo de configuração
├── .env                          # Variáveis de ambiente
├── README.md                     # Este ficheiro
├── start_airflow.sh              # Script de arranque
├── stop_airflow.sh               # Script de paragem
├── restart_airflow.sh            # Script de reinício
├── config/                       # Configurações
│   ├── gcs-credentials.json      # Credenciais GCS (opcional)
│   └── gcs_env.sh               # Variáveis ambiente GCS
├── dags/                         # DAGs Airflow
│   ├── daily_eventos_processing_dag.py
│   ├── pipeline_scraper_google.py
│   └── debug_coordinates_dag.py
├── scripts/                      # Scripts de apoio
│   ├── docker-entrypoint.sh     # Script de entrada Docker
│   ├── init_gcs.sh              # Inicialização GCS
│   └── google_scraper/          # Módulos scraping
│       ├── scraping/            # Lógica de scraping
│       ├── processador/         # Processamento dados
│       ├── exportador_gcs/      # Exportação GCS
│       ├── extracao/           # Extracção conteúdo
│       └── utils/              # Utilitários
├── data/                        # Dados processados
│   ├── raw/                    # Dados brutos
│   ├── structured/             # Dados estruturados
│   └── processed/              # Dados processados
├── logs/                       # Logs Airflow
└── plugins/                    # Plugins Airflow
```

## 📊 Monitorização e Logs

### Interface Web Airflow

- **DAGs**: Visão geral de todos os pipelines
- **Task Instances**: Estado de execução das tarefas
- **Logs**: Logs detalhados por tarefa
- **Gantt Chart**: Visualização temporal de execução

### Logs do Sistema

```bash
# Logs do Airflow
docker compose logs airflow-standalone

# Logs da base de dados
docker compose logs postgres

# Logs em tempo real
docker compose logs -f airflow-standalone
```

### Logs de Dados

Os dados processados são organizados por data:
```
data/
├── raw/2025/06/05/           # Dados brutos
├── structured/2025/06/05/    # Dados estruturados
└── processed/2025/06/05/     # Dados processados
```

## 🔧 Resolução de Problemas

### Problema: Contentor não arranca

```bash
# Verificar status dos contentores
docker compose ps

# Verificar logs para erros
docker compose logs airflow-standalone

# Reinicializar completamente
docker compose down -v
./start_airflow.sh
```

### Problema: Erro de permissões

```bash
# Verificar e corrigir permissões
sudo chown -R $(id -u):$(id -g) ./data ./logs ./config
chmod -R 755 ./data ./logs ./config
```

### Problema: Credenciais GCS

```bash
# Verificar configuração
./scripts/debug_env.sh

# Validar credenciais
gcloud auth list
gcloud auth application-default login
```

### Problema: Ligação à base de dados

```bash
# Testar ligação
docker compose exec airflow-standalone python3 -c "
import psycopg2
conn = psycopg2.connect(
    host='aws-0-eu-west-3.pooler.supabase.com',
    port=6543,
    user='postgres.kyrfsylobmsdjlrrpful',
    password='HXU3tLVVXRa1jtjo',
    database='postgres'
)
print('✅ Ligação PostgreSQL bem-sucedida')
"
```

## 💡 Comandos Úteis

### Gestão de Contentores

```bash
# Ver status dos serviços
docker compose ps

# Aceder ao shell do contentor Airflow
docker compose exec airflow-standalone bash

# Reconstruir imagens
docker compose build --no-cache

# Limpar tudo (cuidado: remove dados)
docker compose down -v
docker system prune -a
```

### Testes e Validação

```bash
# Testar providers instalados
chmod +x scripts/test_providers.sh
./scripts/test_providers.sh

# Validar configuração GCS
docker compose exec airflow-standalone python3 /opt/airflow/scripts/google_scraper/exportador_gcs/validate_gcs_setup.py

# Debug de ambiente
./scripts/debug_env.sh
```

### Gestão de Dados

```bash
# Limpar dados antigos (manter últimos 7 dias)
find ./data -type f -mtime +7 -delete

# Fazer backup dos dados
tar -czf backup_$(date +%Y%m%d).tar.gz ./data ./config

# Ver tamanho dos dados
du -sh ./data/*
```

## 🔄 Fluxo de Trabalho Típico

1. **Arrancar o sistema**: `./start_airflow.sh`
2. **Aceder à interface**: http://localhost:8080
3. **Activar DAGs**: Toggle nos DAGs desejados
4. **Monitorizar execução**: Ver logs e estado das tarefas
5. **Verificar dados**: Consultar ficheiros em `./data/`
6. **Verificar GCS**: Confirmar upload no bucket GCS

## 🤝 Contribuição

### Desenvolvimento Local

1. Fazer fork do repositório
2. Criar branch para a funcionalidade: `git checkout -b feature/nova-funcionalidade`
3. Fazer alterações e testar localmente
4. Fazer commit: `git commit -m "Descrição da alteração"`
5. Push para o branch: `git push origin feature/nova-funcionalidade`
6. Criar Pull Request

### Padrões de Código

- **Python**: Seguir PEP 8
- **DAGs**: Documentar todas as tarefas
- **Commits**: Mensagens descritivas em português
- **Logs**: Usar logging adequado com níveis apropriados

## 📞 Suporte

Para questões e suporte:

1. **Logs**: Consultar primeiro os logs do sistema
2. **Documentação**: Verificar este README
3. **Issues**: Criar issue no repositório com:
   - Descrição do problema
   - Logs relevantes
   - Passos para reproduzir
   - Ambiente (OS, Docker version, etc.)

---

**SIMPREDE** - Sistema Integrado de Monitorização e Prevenção de Desastres  

