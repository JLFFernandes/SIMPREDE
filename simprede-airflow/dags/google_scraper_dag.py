#!/usr/bin/env python3
"""
SIMPREDE Pipeline de Scraping de Notícias do Google
Compatível com Airflow 3.0.1 - Usando scraper optimizado para Airflow
"""
import os
import sys
import subprocess
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Default arguments
default_args = {
    'owner': 'simprede',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,  # Avoid retry buffering
    'retry_delay': timedelta(minutes=5),
    'do_xcom_push': False,  # Reduces buffering and forces immediate log output
}

# Define the DAG
dag = DAG(
    'pipeline_scraper_google',
    default_args=default_args,
    description='Pipeline completo de scraping e processamento de notícias do Google (optimizado para Airflow)',
    schedule="@daily",  # Updated for Airflow 3.x
    start_date=datetime(2025, 6, 1),
    catchup=False,
    tags=['google', 'scraping', 'noticias', 'simprede', 'airflow-optimizado'],
    max_active_runs=1,
    doc_md=__doc__,
)

def run_scraper_task(**context):
    """Execute the Airflow-optimized scraper using subprocess method"""
    print("🚀 Starting Airflow-Optimized Google News Scraper")
    
    # Ensure data directory exists
    data_dir = '/opt/airflow/scripts/google_scraper/data/raw'
    os.makedirs(data_dir, exist_ok=True)
    
    # Get parameters from DAG run configuration - FIXED DEFAULT TIMEOUT
    dias = context['dag_run'].conf.get('dias', 1) if context['dag_run'].conf else 1
    date = context['dag_run'].conf.get('date', '') if context['dag_run'].conf else ''
    max_time = context['dag_run'].conf.get('max_execution_time', 3600) if context['dag_run'].conf else 3600  # Changed from 1200 to 3600
    
    print(f"📅 Scraper Parameters:")
    print(f"  - Days back: {dias}")
    print(f"  - Specific date: {date if date else 'None (use days_back)'}")
    print(f"  - Max execution time: {max_time} seconds ({max_time/60:.1f} minutes)")
    
    try:
        # Use subprocess method to avoid import issues during DAG scanning
        cmd = [
            sys.executable, '/opt/airflow/scripts/google_scraper/scraping/run_scraper_airflow.py',
            '--dias', str(dias),
            '--max_time', str(max_time),
            '--debug'  # Add debug flag to get more detailed logs
        ]
        
        if date:
            cmd.extend(['--date', date])
        
        print(f"🚀 Running Airflow scraper command: {' '.join(cmd)}")
        print(f"⏰ Process timeout will be: {max_time + 300} seconds ({(max_time + 300)/60:.1f} minutes)")
        
        # Set working directory and environment
        env = os.environ.copy()
        env['PYTHONPATH'] = '/opt/airflow/scripts/google_scraper:' + env.get('PYTHONPATH', '')
        env['PYTHONUNBUFFERED'] = '1'  # This ensures Python output is unbuffered for real-time logs
        
        # Run with real-time output capture using Popen instead of subprocess.run
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd='/opt/airflow/scripts/google_scraper/scraping',
            env=env,
            bufsize=1  # Line buffered
        )
        
        # Capture and print output in real-time
        stdout_lines = []
        stderr_lines = []
        
        # Function to read from a pipe and print/store output
        def read_pipe(pipe, lines_list, prefix):
            for line in iter(pipe.readline, ''):
                if line:
                    print(f"{prefix} {line.rstrip()}")  # Print directly to Airflow logs
                    lines_list.append(line)
            pipe.close()
        
        # Create threads to read stdout and stderr
        import threading
        stdout_thread = threading.Thread(
            target=read_pipe, 
            args=(process.stdout, stdout_lines, "📋")
        )
        stderr_thread = threading.Thread(
            target=read_pipe, 
            args=(process.stderr, stderr_lines, "⚠️")
        )
        
        # Start threads and wait for them to finish
        stdout_thread.start()
        stderr_thread.start()
        
        # Wait for process to complete with timeout
        try:
            timeout_with_buffer = max_time + 300  # Add 5 minutes buffer
            print(f"⏰ Waiting for process completion with timeout: {timeout_with_buffer} seconds")
            
            exit_code = process.wait(timeout=timeout_with_buffer)
            stdout_thread.join()
            stderr_thread.join()
            
            stdout_content = ''.join(stdout_lines)
            stderr_content = ''.join(stderr_lines)
            
            if exit_code != 0:
                print(f"❌ Airflow scraper failed with return code {exit_code}")
                raise Exception(f"Airflow scraper failed with return code {exit_code}")
            
            print("✅ Airflow scraper completed successfully!")
            return stdout_content
            
        except subprocess.TimeoutExpired:
            process.kill()
            print(f"⏰ Scraper timed out after {timeout_with_buffer} seconds ({timeout_with_buffer/60:.1f} minutes)")
            print(f"⏰ Original max_time was: {max_time} seconds ({max_time/60:.1f} minutes)")
            raise Exception(f"Scraper execution exceeded maximum time limit of {timeout_with_buffer} seconds")
        
    except Exception as e:
        print(f"❌ Unexpected error in scraper task: {str(e)}")
        
        # Fallback to original scraper - ALSO INCREASE TIMEOUT HERE
        print("🔄 Fallback: using original run_scraper.py...")
        
        cmd = [
            'python', '/opt/airflow/scripts/google_scraper/scraping/run_scraper.py',
            '--dias', str(dias)
        ]
        
        if date:
            cmd.extend(['--date', date])
        
        print(f"🚀 Running fallback scraper command: {' '.join(cmd)}")
        
        # Increase fallback timeout to match main timeout
        fallback_timeout = max(1800, max_time)  # At least 30 minutes or max_time, whichever is higher
        print(f"⏰ Fallback timeout: {fallback_timeout} seconds ({fallback_timeout/60:.1f} minutes)")
        
        result = subprocess.run(
            cmd, 
            capture_output=True, 
            text=True, 
            cwd='/opt/airflow/scripts/google_scraper', 
            timeout=fallback_timeout  # Use dynamic timeout instead of fixed 1800
        )
        
        print(f"📋 Fallback scraper stdout: {result.stdout}")
        
        if result.stderr:
            print(f"⚠️ Fallback scraper stderr: {result.stderr}")
        
        if result.returncode != 0:
            raise Exception(f"All scraper methods failed. Last error: {result.stderr}")
        
        print("✅ Fallback scraper completed successfully")
        return result.stdout

def processar_relevantes_task(**context):
    """Process relevant articles with real-time logging"""
    print("🔄 Starting processar_relevantes task")
    
    # Get today's date for file paths (not yesterday's)
    today = datetime.now()
    dias = context['dag_run'].conf.get('dias', 1) if context['dag_run'].conf else 1
    date = context['dag_run'].conf.get('date', '') if context['dag_run'].conf else ''
    
    cmd = [
        'python', '/opt/airflow/scripts/google_scraper/processador/processar_relevantes_airflow.py',
        '--dias', str(dias)
    ]
    
    # Pass today's date if no specific date is provided
    if date:
        cmd.extend(['--date', date])
    else:
        # Use today's date instead of calculating backwards
        cmd.extend(['--date', today.strftime('%Y-%m-%d')])
    
    print(f"🔄 Running processing command: {' '.join(cmd)}")
    
    # Set working directory and environment for real-time output
    env = os.environ.copy()
    env['PYTHONPATH'] = '/opt/airflow/scripts/google_scraper:' + env.get('PYTHONPATH', '')
    env['PYTHONUNBUFFERED'] = '1'  # This ensures Python output is unbuffered for real-time logs
    
    # Run with real-time output capture using Popen instead of subprocess.run
    import threading
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd='/opt/airflow/scripts/google_scraper/processador',
        env=env,
        bufsize=1  # Line buffered
    )
    
    # Capture and print output in real-time
    stdout_lines = []
    stderr_lines = []
    
    # Function to read from a pipe and print/store output
    def read_pipe(pipe, lines_list, prefix):
        for line in iter(pipe.readline, ''):
            if line:
                print(f"{prefix} {line.rstrip()}")  # Print directly to Airflow logs
                lines_list.append(line)
        pipe.close()
    
    # Create threads to read stdout and stderr
    stdout_thread = threading.Thread(
        target=read_pipe, 
        args=(process.stdout, stdout_lines, "📋")
    )
    stderr_thread = threading.Thread(
        target=read_pipe, 
        args=(process.stderr, stderr_lines, "⚠️")
    )
    
    # Start threads and wait for them to finish
    stdout_thread.start()
    stderr_thread.start()
    
    # Wait for process to complete with timeout
    try:
        timeout = 1800  # 30 minutes timeout for processing
        print(f"⏰ Waiting for processing completion with timeout: {timeout} seconds")
        
        exit_code = process.wait(timeout=timeout)
        stdout_thread.join()
        stderr_thread.join()
        
        stdout_content = ''.join(stdout_lines)
        stderr_content = ''.join(stderr_lines)
        
        if exit_code != 0:
            print(f"❌ Processing failed with return code {exit_code}")
            if stderr_content:
                print(f"❌ Error details: {stderr_content}")
            raise Exception(f"Processing failed with return code {exit_code}: {stderr_content}")
        
        print("✅ Processing completed successfully!")
        
        # Check if relevant articles file was created
        today_str = datetime.now().strftime('%Y%m%d')
        expected_file = f"/opt/airflow/scripts/google_scraper/data/raw/{datetime.now().strftime('%Y/%m/%d')}/artigos_google_municipios_pt_{today_str}.csv"
        
        print(f"🔍 Looking for processed articles file: {expected_file}")
        if os.path.exists(expected_file):
            print(f"✅ Processed articles file created: {expected_file}")
            # Log file size and row count
            file_size = os.path.getsize(expected_file)
            print(f"📊 File size: {file_size} bytes")
            
            try:
                import pandas as pd
                df = pd.read_csv(expected_file)
                print(f"📊 Number of processed articles: {len(df)}")
            except Exception as e:
                print(f"⚠️ Could not read CSV file: {e}")
        else:
            print("❌ Expected processed articles file not found!")
            # List all files in the directory
            base_dir = os.path.dirname(expected_file)
            if os.path.exists(base_dir):
                print(f"📁 Files in {base_dir}: {os.listdir(base_dir)}")
            else:
                print(f"📁 Directory {base_dir} does not exist")
        
        return stdout_content
        
    except subprocess.TimeoutExpired:
        process.kill()
        print(f"⏰ Processing timed out after {timeout} seconds ({timeout/60:.1f} minutes)")
        raise Exception(f"Processing execution exceeded maximum time limit of {timeout} seconds")
    except Exception as e:
        print(f"❌ Unexpected error in processing task: {str(e)}")
        raise

def filtrar_vitimas_task(**context):
    """Filter articles with victim information using the new filtrar_artigos_vitimas_airflow.py"""
    print("🔄 Starting filtrar_vitimas task")
    
    dias = context['dag_run'].conf.get('dias', 1) if context['dag_run'].conf else 1
    date = context['dag_run'].conf.get('date', '') if context['dag_run'].conf else ''
    
    # Check if input file exists before running
    today = datetime.now()
    today_str = today.strftime('%Y%m%d')
    year_str = today.strftime('%Y')
    month_str = today.strftime('%m')
    day_str = today.strftime('%d')
    date_suffix = today.strftime('%Y-%m-%d')  # Format used by processar_relevantes
    
    # Look for the output from processar_relevantes_airflow in multiple locations
    possible_input_files = [
        # Structured directory with date suffix (most likely location)
        f"/opt/airflow/data/structured/{year_str}/{month_str}/{day_str}/artigos_google_municipios_pt_{date_suffix}.csv",
        # Raw directory with date suffix
        f"/opt/airflow/data/raw/{year_str}/{month_str}/{day_str}/artigos_google_municipios_pt_{date_suffix}.csv",
        # Legacy format in raw directory
        f"/opt/airflow/scripts/google_scraper/data/raw/{year_str}/{month_str}/{day_str}/artigos_google_municipios_pt_{today_str}.csv",
        # Fallback to structured directory
        f"/opt/airflow/scripts/google_scraper/data/structured/artigos_google_municipios_pt_{date_suffix}.csv"
    ]
    
    input_file = None
    for possible_file in possible_input_files:
        print(f"🔍 Checking for input file: {possible_file}")
        if os.path.exists(possible_file):
            input_file = possible_file
            print(f"✅ Found input file: {input_file}")
            break
    
    if not input_file:
        print("❌ No input file found in any expected location")
        # Try to find any processed articles files
        search_dirs = [
            f"/opt/airflow/scripts/google_scraper/data/structured/{year_str}/{month_str}/{day_str}",
            f"/opt/airflow/scripts/google_scraper/data/raw/{year_str}/{month_str}/{day_str}",
            "/opt/airflow/scripts/google_scraper/data/structured",
            "/opt/airflow/scripts/google_scraper/data/raw"
        ]
        
        for search_dir in search_dirs:
            if os.path.exists(search_dir):
                files = [f for f in os.listdir(search_dir) if 'artigos_google_municipios_pt' in f and f.endswith('.csv')]
                if files:
                    print(f"📁 Available files in {search_dir}: {files}")
                    # Use the first matching file found
                    input_file = os.path.join(search_dir, files[0])
                    print(f"🔄 Using file: {input_file}")
                    break
            else:
                print(f"📁 Directory {search_dir} does not exist")
        
        if not input_file:
            # Try to walk through all directories to find any relevant files
            parent_dir = "/opt/airflow/scripts/google_scraper/data"
            if os.path.exists(parent_dir):
                print(f"📁 Searching all subdirectories in {parent_dir}:")
                for root, dirs, files in os.walk(parent_dir):
                    for file in files:
                        if 'artigos_google_municipios_pt' in file and file.endswith('.csv'):
                            full_path = os.path.join(root, file)
                            print(f"  Found: {full_path}")
                            if not input_file:  # Use the first one found
                                input_file = full_path
                                print(f"🔄 Will use: {input_file}")
            
            if not input_file:
                raise Exception(f"Input file not found in any location. Searched: {possible_input_files}. Please check if the previous task completed successfully.")
    
    cmd = [
        'python', '/opt/airflow/scripts/google_scraper/processador/filtrar_artigos_vitimas_airflow.py',
        '--dias', str(dias)
    ]
    
    if date:
        cmd.extend(['--date', date])
    else:
        # Use today's date instead of calculating backwards
        cmd.extend(['--date', today.strftime('%Y-%m-%d')])
    
    print(f"🔄 Running filtering command: {' '.join(cmd)}")
    
    # Set working directory and environment for real-time output
    env = os.environ.copy()
    env['PYTHONPATH'] = '/opt/airflow/scripts/google_scraper:' + env.get('PYTHONPATH', '')
    env['PYTHONUNBUFFERED'] = '1'  # This ensures Python output is unbuffered for real-time logs
    
    # Run with real-time output capture using Popen instead of subprocess.run
    import threading
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd='/opt/airflow/scripts/google_scraper/processador',
        env=env,
        bufsize=1  # Line buffered
    )
    
    # Capture and print output in real-time
    stdout_lines = []
    stderr_lines = []
    
    # Function to read from a pipe and print/store output
    def read_pipe(pipe, lines_list, prefix):
        for line in iter(pipe.readline, ''):
            if line:
                print(f"{prefix} {line.rstrip()}")  # Print directly to Airflow logs
                lines_list.append(line)
        pipe.close()
    
    # Create threads to read stdout and stderr
    stdout_thread = threading.Thread(
        target=read_pipe, 
        args=(process.stdout, stdout_lines, "📋")
    )
    stderr_thread = threading.Thread(
        target=read_pipe, 
        args=(process.stderr, stderr_lines, "⚠️")
    )
    
    # Start threads and wait for them to finish
    stdout_thread.start()
    stderr_thread.start()
    
    # Wait for process to complete with timeout
    try:
        timeout = 900  # 15 minutes timeout for filtering
        print(f"⏰ Waiting for filtering completion with timeout: {timeout} seconds")
        
        exit_code = process.wait(timeout=timeout)
        stdout_thread.join()
        stderr_thread.join()
        
        stdout_content = ''.join(stdout_lines)
        stderr_content = ''.join(stderr_lines)
        
        if exit_code != 0:
            print(f"❌ Filtering failed with return code {exit_code}")
            if stderr_content:
                print(f"❌ Error details: {stderr_content}")
            raise Exception(f"Filtering failed with return code {exit_code}: {stderr_content}")
        
        print("✅ Filtering completed successfully!")
        
        # Check if filtered articles file was created
        expected_file = f"/opt/airflow/scripts/google_scraper/data/structured/{year_str}/{month_str}/{day_str}/artigos_vitimas_filtrados_{today_str}.csv"
        
        print(f"🔍 Looking for filtered articles file: {expected_file}")
        if os.path.exists(expected_file):
            print(f"✅ Filtered articles file created: {expected_file}")
            # Log file size and row count
            file_size = os.path.getsize(expected_file)
            print(f"📊 File size: {file_size} bytes")
            
            try:
                import pandas as pd
                df = pd.read_csv(expected_file)
                print(f"📊 Number of filtered articles with victims: {len(df)}")
            except Exception as e:
                print(f"⚠️ Could not read CSV file: {e}")
        else:
            print("❌ Expected filtered articles file not found!")
            # List all files in the directory
            base_dir = os.path.dirname(expected_file)
            if os.path.exists(base_dir):
                print(f"📁 Files in {base_dir}: {os.listdir(base_dir)}")
            else:
                print(f"📁 Directory {base_dir} does not exist")
                # Check for compatibility file
                compat_file = "/opt/airflow/scripts/google_scraper/data/structured/artigos_vitimas_filtrados.csv"
                if os.path.exists(compat_file):
                    print(f"✅ Found compatibility file: {compat_file}")
        
        return stdout_content
        
    except subprocess.TimeoutExpired:
        process.kill()
        print(f"⏰ Filtering timed out after {timeout} seconds ({timeout/60:.1f} minutes)")
        raise Exception(f"Filtering execution exceeded maximum time limit of {timeout} seconds")
    except Exception as e:
        print(f"❌ Unexpected error in filtering task: {str(e)}")
        raise

def export_supabase_task(**context):
    """Export to Supabase with improved error handling and path resolution"""
    print("🔄 Starting export_supabase task")
    
    today = datetime.now()
    dias = context['dag_run'].conf.get('dias', 1) if context['dag_run'].conf else 1
    date = context['dag_run'].conf.get('date', '') if context['dag_run'].conf else ''
    
    cmd = ['python', '/opt/airflow/scripts/google_scraper/exportador_bd/export_to_supabase_airflow.py']
    
    # Use consistent date format
    if date:
        cmd.extend(['--date', date])
    else:
        # Use today's date instead of calculating backwards
        cmd.extend(['--date', today.strftime('%Y-%m-%d')])
    
    print(f"🔄 Running export command: {' '.join(cmd)}")
    
    # Set working directory and environment for real-time output
    env = os.environ.copy()
    env['PYTHONPATH'] = '/opt/airflow/scripts/google_scraper:' + env.get('PYTHONPATH', '')
    env['PYTHONUNBUFFERED'] = '1'
    
    # Copy database credentials to environment
    env.update({
        'DB_HOST': 'aws-0-eu-west-3.pooler.supabase.com',
        'DB_PORT': '6543',
        'DB_NAME': 'postgres',
        'DB_USER': 'postgres.kyrfsylobmsdjlrrpful',
        'DB_PASSWORD': 'HXU3tLVVXRa1jtjo',
        'DB_SSLMODE': 'require',
        'DB_SCHEMA': 'google_scraper'  # 🎯 SCHEMA IS SET HERE
    })
    
    # Run with real-time output capture
    import threading
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd='/opt/airflow/scripts/google_scraper/exportador_bd',
        env=env,
        bufsize=1
    )
    
    # Capture and print output in real-time
    stdout_lines = []
    stderr_lines = []
    
    def read_pipe(pipe, lines_list, prefix):
        for line in iter(pipe.readline, ''):
            if line:
                print(f"{prefix} {line.rstrip()}")
                lines_list.append(line)
        pipe.close()
    
    stdout_thread = threading.Thread(
        target=read_pipe, 
        args=(process.stdout, stdout_lines, "📋")
    )
    stderr_thread = threading.Thread(
        target=read_pipe, 
        args=(process.stderr, stderr_lines, "⚠️")
    )
    
    stdout_thread.start()
    stderr_thread.start()
    
    try:
        timeout = 600  # 10 minutes timeout for export
        print(f"⏰ Waiting for export completion with timeout: {timeout} seconds")
        
        exit_code = process.wait(timeout=timeout)
        stdout_thread.join()
        stderr_thread.join()
        
        stdout_content = ''.join(stdout_lines)
        stderr_content = ''.join(stderr_lines)
        
        if exit_code != 0:
            print(f"❌ Export failed with return code {exit_code}")
            if stderr_content:
                print(f"❌ Error details: {stderr_content}")
            raise Exception(f"Export failed with return code {exit_code}: {stderr_content}")
        
        print("✅ Export completed successfully!")
        return stdout_content
        
    except subprocess.TimeoutExpired:
        process.kill()
        print(f"⏰ Export timed out after {timeout} seconds")
        raise Exception(f"Export execution exceeded maximum time limit of {timeout} seconds")
    except Exception as e:
        print(f"❌ Unexpected error in export task: {str(e)}")
        raise

# Create a task to check data directory and dependencies
verificar_directorio_dados = BashOperator(
    task_id='verificar_directorio_dados',
    bash_command='''
    echo "📁 A criar directorias de dados..."
    mkdir -p /opt/airflow/scripts/google_scraper/data/raw/$(date +%Y/%m/%d)
    mkdir -p /opt/airflow/scripts/google_scraper/data/processed/$(date +%Y/%m/%d)
    
    echo "✅ Directorias de dados criadas"
    echo "📋 A verificar ficheiros do scraper..."
    
    if [ -f "/opt/airflow/scripts/google_scraper/scraping/run_scraper_airflow.py" ]; then
        echo "✅ Encontrado run_scraper_airflow.py"
    else
        echo "❌ Em falta run_scraper_airflow.py"
        ls -la /opt/airflow/scripts/google_scraper/scraping/ || echo "Directoria de scraping não encontrada"
    fi
    
    if [ -f "/opt/airflow/scripts/google_scraper/config/keywords.json" ]; then
        echo "✅ Encontrado keywords.json"
    else
        echo "❌ Em falta keywords.json"
    fi
    
    if [ -f "/opt/airflow/scripts/google_scraper/config/municipios_por_distrito.json" ]; then
        echo "✅ Encontrado municipios_por_distrito.json"
    else
        echo "❌ Em falta municipios_por_distrito.json"
    fi
    
    echo "📋 Estrutura actual das directorias:"
    find /opt/airflow/scripts/google_scraper -type f -name "*.py" | head -10 || echo "Ficheiros Python não encontrados"
    ''',
    dag=dag,
)

# Define tasks
executar_scraper = PythonOperator(
    task_id='executar_scraper_airflow',
    python_callable=run_scraper_task,
    dag=dag,
)

processar_relevantes = PythonOperator(
    task_id='processar_relevantes',
    python_callable=processar_relevantes_task,
    dag=dag,
)

filtrar_artigos_vitimas = PythonOperator(
    task_id='filtrar_artigos_vitimas',
    python_callable=filtrar_vitimas_task,
    dag=dag,
)

exportar_para_supabase = PythonOperator(
    task_id='exportar_para_supabase',
    python_callable=export_supabase_task,
    dag=dag,
)

# Set dependencies
verificar_directorio_dados >> executar_scraper >> processar_relevantes >> filtrar_artigos_vitimas >> exportar_para_supabase
