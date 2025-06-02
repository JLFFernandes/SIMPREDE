#!/usr/bin/env python3
"""
Health check script for SIMPREDE project
Verifies that all components are working correctly before running the pipeline
"""

import os
import sys
import subprocess
import requests
from pathlib import Path

def check_python_environment():
    """Check Python environment and required packages"""
    print("🐍 Checking Python environment...")
    
    required_packages = [
        'selenium', 'beautifulsoup4', 'requests', 'pandas', 
        'numpy', 'spacy', 'sklearn', 'supabase'
    ]
    
    missing_packages = []
    for package in required_packages:
        try:
            __import__(package)
            print(f"  ✅ {package}")
        except ImportError:
            missing_packages.append(package)
            print(f"  ❌ {package}")
    
    if missing_packages:
        print(f"Missing packages: {missing_packages}")
        return False
    
    return True

def check_config_files():
    """Check if all required configuration files exist"""
    print("📁 Checking configuration files...")
    
    config_files = [
        'config/dados_treino.json',
        'config/eventos_climaticos.json',
        'config/keywords.json',
        'config/municipios_por_distrito.json',
        'config/user_agents.txt'
    ]
    
    missing_files = []
    for config_file in config_files:
        if os.path.exists(config_file):
            print(f"  ✅ {config_file}")
        else:
            missing_files.append(config_file)
            print(f"  ❌ {config_file}")
    
    if missing_files:
        print(f"Missing config files: {missing_files}")
        return False
    
    return True

def check_models():
    """Check if trained models exist"""
    print("🤖 Checking trained models...")
    
    model_files = [
        'models/modelo_classificacao.pkl',
        'models/tfidf_vectorizer.pkl',
        'models/victims_nlp/'
    ]
    
    missing_models = []
    for model_file in model_files:
        if os.path.exists(model_file):
            print(f"  ✅ {model_file}")
        else:
            missing_models.append(model_file)
            print(f"  ❌ {model_file}")
    
    if missing_models:
        print(f"Missing models: {missing_models}")
        return False
    
    return True

def check_data_directories():
    """Check if data directories exist and are writable"""
    print("📂 Checking data directories...")
    
    directories = [
        'data/raw',
        'data/structured'
    ]
    
    for directory in directories:
        if os.path.exists(directory):
            if os.access(directory, os.W_OK):
                print(f"  ✅ {directory} (writable)")
            else:
                print(f"  ⚠️  {directory} (not writable)")
                return False
        else:
            print(f"  ❌ {directory} (doesn't exist)")
            # Create directory
            os.makedirs(directory, exist_ok=True)
            print(f"  ✅ Created {directory}")
    
    return True

def check_database_connection():
    """Check connection to Supabase database"""
    print("🗄️  Checking database connection...")
    
    try:
        from supabase import create_client
        import configparser
        
        # Try to read configuration
        config = configparser.ConfigParser()
        if os.path.exists('config.cfg'):
            config.read('config.cfg')
            
            supabase_url = config.get('supabase', 'url', fallback=None)
            supabase_key = config.get('supabase', 'key', fallback=None)
            
            if supabase_url and supabase_key:
                supabase_client = create_client(supabase_url, supabase_key)
                # Simple test query
                result = supabase_client.table('noticias').select('count').limit(1).execute()
                print("  ✅ Database connection successful")
                return True
            else:
                print("  ⚠️  Supabase configuration not found in config.cfg")
        
        # Fallback to environment variables
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        db_host = os.getenv('DB_HOST')
        
        if db_user and db_password and db_host:
            print("  ✅ Database credentials found in environment")
            return True
        else:
            print("  ❌ Database credentials not configured")
            return False
            
    except Exception as e:
        print(f"  ❌ Database connection failed: {e}")
        return False

def check_internet_connectivity():
    """Check internet connectivity for web scraping"""
    print("🌐 Checking internet connectivity...")
    
    test_urls = [
        'https://news.google.com',
        'https://www.google.com'
    ]
    
    for url in test_urls:
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                print(f"  ✅ {url}")
            else:
                print(f"  ⚠️  {url} (status {response.status_code})")
        except Exception as e:
            print(f"  ❌ {url} ({e})")
            return False
    
    return True

def main():
    """Run all health checks"""
    print("🏥 SIMPREDE Health Check")
    print("=" * 50)
    
    checks = [
        ("Python Environment", check_python_environment),
        ("Configuration Files", check_config_files),
        ("Trained Models", check_models),
        ("Data Directories", check_data_directories),
        ("Database Connection", check_database_connection),
        ("Internet Connectivity", check_internet_connectivity)
    ]
    
    all_passed = True
    results = []
    
    for check_name, check_function in checks:
        print(f"\n{check_name}:")
        try:
            result = check_function()
            results.append((check_name, result))
            if not result:
                all_passed = False
        except Exception as e:
            print(f"  ❌ Error during {check_name}: {e}")
            results.append((check_name, False))
            all_passed = False
    
    print("\n" + "=" * 50)
    print("📊 Health Check Summary:")
    
    for check_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {check_name}: {status}")
    
    if all_passed:
        print("\n🎉 All health checks passed! System is ready.")
        sys.exit(0)
    else:
        print("\n⚠️  Some health checks failed. Please address the issues above.")
        sys.exit(1)

if __name__ == "__main__":
    main()
