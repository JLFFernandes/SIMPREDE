# SIMPREDE Airflow Container

This directory contains the necessary files to run Apache Airflow in a containerized environment for the SIMPREDE project. The setup includes the Google News Scraper pipeline and related components.

## Prerequisites

- Docker and Docker Compose installed on your system
- At least 4GB of RAM allocated to Docker
- At least 2 CPU cores allocated to Docker
- At least 10GB of free disk space

## Directory Structure

- `dags/`: Contains all the Airflow DAGs, including the Google scraper
- `logs/`: Directory for Airflow logs
- `plugins/`: Directory for Airflow plugins
- `data/`: Directory for storing raw and structured data

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
- For other issues, check the logs in the `logs/` directory or via the Airflow web interface.

## Additional Notes

- The Google scraper is configured to run in headless mode by default. This can be changed in the `.env` file.
- To update the Python dependencies, modify the `requirements.txt` file and rebuild the container.
