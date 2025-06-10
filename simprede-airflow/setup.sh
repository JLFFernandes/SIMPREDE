#!/bin/bash

# SIMPREDE Airflow Setup Script
# This script helps you set up the required environment variables for Supabase connection

echo "🚀 SIMPREDE Airflow Environment Setup"
echo "====================================="

# Check if .env file exists
if [ -f ".env" ]; then
    echo "✅ .env file found"
    echo "🔍 Checking for required Supabase variables..."
    
    # Check each required variable
    missing_vars=()
    
    if ! grep -q "^DB_USER=" .env || grep -q "^DB_USER=your_supabase_username" .env; then
        missing_vars+=("DB_USER")
    fi
    
    if ! grep -q "^DB_PASSWORD=" .env || grep -q "^DB_PASSWORD=your_supabase_password" .env; then
        missing_vars+=("DB_PASSWORD")
    fi
    
    if ! grep -q "^DB_HOST=" .env || grep -q "^DB_HOST=db.your-project-ref.supabase.co" .env; then
        missing_vars+=("DB_HOST")
    fi
    
    if [ ${#missing_vars[@]} -eq 0 ]; then
        echo "✅ All required Supabase variables are configured!"
        echo "🐳 You can now start the containers:"
        echo "   docker compose up"
    else
        echo "❌ Missing or placeholder values for: ${missing_vars[*]}"
        echo "📝 Please edit .env and set these variables to your actual Supabase values"
        echo "🔗 Get your Supabase credentials from:"
        echo "   https://supabase.com/dashboard/project/YOUR_PROJECT/settings/database"
    fi
else
    echo "❌ .env file not found"
    echo "📋 Creating .env from template..."
    
    if [ -f ".env.example" ]; then
        cp .env.example .env
        echo "✅ .env file created from .env.example"
        echo "📝 Please edit .env and add your Supabase credentials:"
        echo "   - DB_USER (usually 'postgres')"
        echo "   - DB_PASSWORD (your Supabase database password)"
        echo "   - DB_HOST (e.g., db.your-ref.supabase.co)"
        echo ""
        echo "🔗 Get these from: https://supabase.com/dashboard/project/YOUR_PROJECT/settings/database"
    else
        echo "❌ .env.example not found. Please create .env manually with required variables."
    fi
fi

echo ""
echo "📚 Next steps:"
echo "1. Edit .env with your Supabase credentials"
echo "2. Start containers: docker compose up"
echo "3. Access Airflow at: http://localhost:8080"
echo "4. Check the sql_queries_pipeline DAG for connection status"