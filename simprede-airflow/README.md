# SIMPREDE Airflow Container

This directory contains the necessary files to run Apache Airflow in a containerized environment for the SIMPREDE project. The setup includes the Google News Scraper pipeline and related components.

## Prerequisites

- Docker and Docker Compose installed on your system
- At least 4GB of RAM allocated to Docker
- At least 2 CPU cores allocated to Docker
- At least 10GB of free disk space

## Quick Start

### 🚀 First Time Setup
```bash
./start_airflow.sh
```
This script will:
- Check Docker prerequisites
- Build the Docker images
- Start all containers
- Display admin credentials
- Show useful commands

### ⚡ Quick Operations
```bash
# Restart containers (no rebuild)
./restart_airflow.sh

# Stop all containers
./stop_airflow.sh
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

- Airflow credentials: Username `airflow`, Password `airflow`
- PostgreSQL database: User `airflow`, Password `airflow`, Database `airflow`
- Other settings for the Google scraper and additional components

## Getting Started

1. Build and start the containers:

   ```bash
   docker-compose up -d
   ```

2. Access the Airflow web interface:
   
   Open your browser and navigate to `http://localhost:8080`
   
   Login with the credentials specified in the `.env` file (default: `airflow`/`airflow`)

3. Enable the DAGs that you want to run

## Stopping the Containers

To stop all containers:

```bash
docker-compose down
```

To stop all containers and remove volumes (this will delete all the data in the PostgreSQL database):

```bash
docker-compose down -v
```

## Troubleshooting

- If you encounter permission issues, ensure that the `AIRFLOW_UID` in the `.env` file is set correctly.
- **Data Dependencies**: Some tasks may skip gracefully when no input data is available (e.g., export tasks when no disaster-related articles are found)
- For other issues, check the logs in the `logs/` directory or via the Airflow web interface.

## Additional Notes

- **Data Persistence**: All scraped data is automatically saved to the `./data/` directory on your host machine
- The Google scraper is configured to run in headless mode by default. This can be changed in the `.env` file.
- To update the Python dependencies, modify the `requirements.txt` file and rebuild the container.
- **Data Location**: Check `./data/raw/`, `./data/structured/`, and `./data/processed/` for extracted files
