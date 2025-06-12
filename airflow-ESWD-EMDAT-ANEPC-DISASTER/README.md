# SIMPREDE Airflow Container

Este diretório contém os ficheiros necessários para executar o Apache Airflow num ambiente conteinerizado para o projeto SIMPREDE. O setup inclui pipelines automatizadas de importação de dados de desastres: EM-DAT, ESWD, PTS e GEOMAI.

## Requisitos

* Docker e Docker Compose instalados
* Pelo menos 4GB de RAM atribuídos ao Docker
* Pelo menos 2 CPUs atribuídas ao Docker
* Mínimo 10GB de espaço livre em disco

## Arranque Rápido

### Setup com um comando

```bash
./start_airflow.sh
```

Este script realiza:

* Validação dos requisitos Docker
* Criação do ficheiro `.env` com configurações padrão
* Construção de imagens Docker
* Arranque dos contentores
* Mostra credenciais de acesso à interface do Airflow


### Credenciais por defeito

* **Utilizador**: `admin`
* **Password**: `simprede`
* **Web UI**: [http://localhost:8080](http://localhost:8080)

## Estrutura 

* `dags/`: DAGs do Airflow (EM-DAT, ESWD, GEOMAI, PTS_Disaster)
* `scripts/`: Scripts Python chamados pelos DAGs
* `include/`: Ficheiros auxiliares (CSV/Excel/shapefiles)

  * `centroides/`: Shapefiles com centroides de distritos, concelhos e freguesias
* `logs/`: Logs gerados pelo Airflow

## Dados Persistentes

Os dados extraídos são guardados na base de dados Supabase configurada via `.env`. Alguns scripts também podem guardar temporariamente ficheiros CSV/XLSX na pasta `temp_downloads/`.

## Configuração

As variáveis de ambiente são definidas no ficheiro `.env`. Exemplo:

```bash
DB_HOST=aws-0-eu-west-3.pooler.supabase.com
DB_PORT=6543
DB_NAME=postgres
DB_USER=postgres.kyrfsylobmsdjlrrpful
DB_PASSWORD=HXU3tLVVXRa1jtjo
DB_SSLMODE=require
```

## Primeiros Passos

1. Iniciar:

```bash
./start_airflow.sh
```

2. Aceder a:

```
http://localhost:8080
```

3. Ativar os DAGs desejados (ex: `emdat_final_dag`, `eswd_final_dag`, `geomai_dag`, `pts_disaster_dag`)


