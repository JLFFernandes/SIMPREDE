#!/bin/bash

# Test script to verify environment variables are loaded correctly in Docker

echo "🔍 Testing Environment Variables in Docker Container"
echo "=================================================="

echo ""
echo "📋 Checking if .env file exists in the project root:"
if [ -f ".env" ]; then
    echo "✅ .env file found"
    echo "📄 .env file content (redacted):"
    grep -E "^(DB_USER|DB_HOST|DB_PORT|DB_NAME)=" .env | sed 's/=.*$/=***REDACTED***/'
else
    echo "❌ .env file not found"
fi

echo ""
echo "🐳 Checking environment variables inside Docker container:"
echo "DB_USER: ${DB_USER:-NOT_SET}"
echo "DB_PASSWORD: ${DB_PASSWORD:+SET}" # Show SET if password exists, otherwise empty
echo "DB_HOST: ${DB_HOST:-NOT_SET}"
echo "DB_PORT: ${DB_PORT:-NOT_SET}"
echo "DB_NAME: ${DB_NAME:-NOT_SET}"

echo ""
echo "🔧 Raw environment variable values (for debugging):"
echo "Raw DB_USER: '${DB_USER}'"
echo "Raw DB_HOST: '${DB_HOST}'"

echo ""
echo "🧹 Environment variables after quote stripping:"
DB_USER_CLEAN=$(echo "${DB_USER}" | sed 's/^["'\'']*\|["'\'']*$//g')
DB_HOST_CLEAN=$(echo "${DB_HOST}" | sed 's/^["'\'']*\|["'\'']*$//g')
echo "Cleaned DB_USER: '${DB_USER_CLEAN}'"
echo "Cleaned DB_HOST: '${DB_HOST_CLEAN}'"

echo ""
if [ -n "${DB_USER_CLEAN}" ] && [ -n "${DB_PASSWORD}" ] && [ -n "${DB_HOST_CLEAN}" ]; then
    echo "✅ All required Supabase credentials are available!"
    echo "🚀 The DAG should be able to connect to Supabase"
else
    echo "❌ Some required credentials are missing:"
    [ -z "${DB_USER_CLEAN}" ] && echo "  - DB_USER is missing or empty"
    [ -z "${DB_PASSWORD}" ] && echo "  - DB_PASSWORD is missing or empty"
    [ -z "${DB_HOST_CLEAN}" ] && echo "  - DB_HOST is missing or empty"
    
    echo ""
    echo "🔧 Troubleshooting steps:"
    echo "1. Restart Docker containers: docker compose down && docker compose up"
    echo "2. Check docker-compose.yml includes .env file loading"
    echo "3. Verify .env file is in the same directory as docker-compose.yml"
fi