# SQL Queries DAG - User Guide

## ✅ Your Setup is Working!

Since your scraper DAG is successfully exporting to Supabase, your SQL queries DAG should now work perfectly. Here's how to use it:

## Available Tasks

### 1. `check_providers`
- Verifies PostgreSQL provider is installed
- Shows available database providers

### 2. `setup_supabase_connection`
- Creates/updates the `supabase_postgres` connection in Airflow
- Uses your environment variables (DB_USER, DB_PASSWORD, DB_HOST, etc.)

### 3. `supabase_health_check`
- Tests basic connectivity to Supabase
- Checks if `google_scraper` schema exists
- Counts staging tables

### 4. `validate_connections`
- Lists all tables in `google_scraper` schema
- Categorizes staging vs permanent tables

### 5. `execute_sql_queries`
- Executes custom SQL queries based on DAG configuration
- Supports both SELECT (fetch results) and INSERT/UPDATE queries

### 6. `create_artigos_filtrados_table`
- Creates the main consolidated table for filtered articles
- Includes proper indexes for performance

### 7. `append_filtered_articles`
- Intelligently appends articles from staging tables to main table
- Automatic false positive filtering (excludes non-Portuguese domains)
- Duplicate prevention

## How to Use

### 1. Basic Health Check
Simply trigger the DAG without any configuration to run a health check on your Supabase connection.

### 2. Custom SQL Queries
Trigger the DAG with configuration to execute custom queries:

```json
{
  "queries": [
    {
      "name": "count_all_articles",
      "sql": "SELECT COUNT(*) FROM google_scraper.artigos_filtrados",
      "fetch_results": true
    },
    {
      "name": "recent_staging_tables",
      "sql": "SELECT table_name FROM information_schema.tables WHERE table_schema = 'google_scraper' AND table_name LIKE '%staging%' ORDER BY table_name DESC LIMIT 5",
      "fetch_results": true
    }
  ]
}
```

### 3. Append Articles from Staging
Process and filter articles from your scraper's staging tables:

```json
{
  "source_table": "artigos_filtrados_20250610_staging",
  "exclude_domains": [".com.br", ".mx", ".ar", ".cl"],
  "dry_run": false
}
```

### 4. Monitor Your Data
Check daily ingestion statistics:

```json
{
  "queries": [
    {
      "name": "daily_stats",
      "sql": "SELECT DATE(data_ingestao) as date, COUNT(*) as articles FROM google_scraper.artigos_filtrados GROUP BY DATE(data_ingestao) ORDER BY date DESC LIMIT 7",
      "fetch_results": true
    },
    {
      "name": "articles_with_victims",
      "sql": "SELECT COUNT(*) FROM google_scraper.artigos_filtrados WHERE has_victims = true",
      "fetch_results": true
    }
  ]
}
```

## Integration with Your Scraper DAG

Your scraper DAG creates staging tables with names like:
- `artigos_filtrados_20250610_staging`

The SQL queries DAG can process these automatically:

1. **Daily Workflow**: After your scraper runs, trigger the SQL DAG to consolidate the data
2. **Filtering**: Removes false positives from non-Portuguese sites
3. **Deduplication**: Prevents duplicate articles in the main table

## Expected Results

When you run the DAG, you should see logs like:

```
✅ Supabase health check successful
📊 Found X staging tables in google_scraper schema
✅ Successfully inserted Y articles from artigos_filtrados_YYYYMMDD_staging
```

## Troubleshooting

### If tasks fail:
1. Check that your scraper DAG has created staging tables
2. Verify the PostgreSQL provider is installed in your Airflow container
3. Confirm environment variables are loaded correctly

### To test manually:
```bash
# Test connection inside the container
docker compose exec airflow-standalone python /opt/airflow/test_sql_connection.py

# Check environment variables
docker compose exec airflow-standalone env | grep DB_
```

## Next Steps

1. **Automate**: Set up the SQL DAG to run after your scraper DAG completes
2. **Monitor**: Use the health check tasks to monitor your data pipeline
3. **Analyze**: Query the consolidated `artigos_filtrados` table for insights
4. **Export**: Use the query results for reports or further analysis

Your setup is now complete and ready for production use! 🚀