# SIMPREDE Airflow Setup Guide

## Quick Setup for Supabase Connection

Your Airflow DAG is failing because the required Supabase environment variables are not set. Here's how to fix it:

### 1. Copy the Environment Template
```bash
cp .env.example .env
```

### 2. Get Your Supabase Credentials

1. Go to your [Supabase Dashboard](https://supabase.com/dashboard)
2. Select your project
3. Go to **Settings** → **Database**
4. In the **Connection info** section, copy:
   - **Host**: `db.your-project-ref.supabase.co`
   - **Database name**: `postgres`
   - **Port**: `6543` (for connection pooling)
   - **User**: `postgres`
   - **Password**: Your database password

### 3. Edit the .env File

Open `.env` and replace the placeholder values:

```bash
# Replace these with your actual Supabase values
DB_USER=postgres
DB_PASSWORD=your_actual_password_here
DB_HOST=db.your-project-ref.supabase.co
DB_PORT=6543
DB_NAME=postgres
```

### 4. Restart Your Containers

```bash
docker compose down
docker compose up
```

### 5. Verify the Connection

1. Go to Airflow UI: http://localhost:8080
2. Find the `sql_queries_pipeline` DAG
3. Trigger the DAG and check the `setup_supabase_connection` task logs
4. You should see: `✅ Created new Supabase connection` or `✅ Updated existing Supabase connection`

## Troubleshooting

### Still seeing "Missing Supabase credentials" error?

1. **Check your .env file exists**:
   ```bash
   ls -la .env
   ```

2. **Verify environment variables are loaded**:
   ```bash
   docker compose exec airflow-standalone env | grep DB_
   ```

3. **Check for typos in variable names** - they must be exactly:
   - `DB_USER`
   - `DB_PASSWORD` 
   - `DB_HOST`
   - `DB_PORT`
   - `DB_NAME`

4. **Ensure no quotes around values** in .env:
   ```bash
   # ✅ Correct
   DB_USER=postgres
   
   # ❌ Wrong
   DB_USER="postgres"
   ```

### Connection test fails?

1. **Check Supabase project is running**
2. **Verify the host URL is correct** (should start with `db.` not `https://`)
3. **Try port 5432** instead of 6543 if connection pooling doesn't work
4. **Check your IP is allowed** in Supabase → Settings → Authentication → URL Configuration

## Alternative Setup Methods

### Method 1: Use the Setup Script
```bash
chmod +x setup.sh
./setup.sh
```

### Method 2: Set Environment Variables Directly
```bash
export DB_USER=postgres
export DB_PASSWORD=your_password
export DB_HOST=db.your-ref.supabase.co
export DB_PORT=6543
export DB_NAME=postgres

docker compose up
```

### Method 3: Edit docker-compose.yml Directly
If you prefer not to use .env files, you can set the values directly in docker-compose.yml:

```yaml
environment:
  DB_USER: postgres
  DB_PASSWORD: your_password
  DB_HOST: db.your-ref.supabase.co
  DB_PORT: 6543
  DB_NAME: postgres
```

## What This Fixes

After setting up the environment variables correctly, your DAG will:

1. ✅ Successfully connect to Supabase
2. ✅ Create/update the `supabase_postgres` connection in Airflow
3. ✅ Run health checks on your database
4. ✅ Execute SQL queries on your `google_scraper` schema
5. ✅ Process and filter articles data

## Need Help?

If you're still having issues:

1. Check the Airflow task logs for specific error messages
2. Verify your Supabase project is active and accessible
3. Test the connection manually using a PostgreSQL client
4. Ensure your Supabase plan supports the number of connections you need