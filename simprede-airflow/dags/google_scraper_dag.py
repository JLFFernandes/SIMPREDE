#!/usr/bin/env python3
"""
SIMPREDE Pipeline de Scraping de Notícias do Google
"""
import os
import sys
import subprocess
import threading
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

class GoogleScraperPaths:
    """Centralized path management for Google Scraper pipeline"""
    
    def __init__(self, base_dir="/opt/airflow", execution_date=None, use_current_date=False):
        self.base_dir = base_dir
        
        # Fix: Use current date when explicitly requested or when no execution_date provided
        if use_current_date or execution_date is None:
            self.execution_date = datetime.now()
            print(f"📅 Using current date: {self.execution_date.strftime('%Y-%m-%d')}")
        else:
            self.execution_date = execution_date
            print(f"📅 Using provided execution date: {self.execution_date.strftime('%Y-%m-%d')}")
        
        # Base directories
        self.scripts_dir = os.path.join(base_dir, "scripts", "google_scraper")
        # Fix: Put data directory inside scripts/google_scraper
        self.data_dir = os.path.join(self.scripts_dir, "data")
        
        # Date-based structure
        self.year = self.execution_date.strftime("%Y")
        self.month = self.execution_date.strftime("%m")
        self.day = self.execution_date.strftime("%d")
        self.date_str = self.execution_date.strftime("%Y%m%d")
        self.date_str_compact = self.date_str  # Add missing property
        self.date_iso = self.execution_date.strftime("%Y-%m-%d")
        
        # Data directories
        self.raw_dir = os.path.join(self.data_dir, "raw", self.year, self.month, self.day)
        self.structured_dir = os.path.join(self.data_dir, "structured", self.year, self.month, self.day)
        self.processed_dir = os.path.join(self.data_dir, "processed", self.year, self.month, self.day)
        
        # Script directories
        self.scraping_dir = os.path.join(self.scripts_dir, "scraping")
        self.processador_dir = os.path.join(self.scripts_dir, "processador")
        self.exportador_dir = os.path.join(self.scripts_dir, "exportador_bd")
        self.config_dir = os.path.join(self.scripts_dir, "config")
        
    def get_scraper_outputs(self):
        """Get all output file paths for the scraper task"""
        return {
            "intermediate_csv": os.path.join(self.raw_dir, f"intermediate_google_news_{self.date_str}.csv"),
            "final_csv": os.path.join(self.raw_dir, f"google_news_articles_{self.date_str}.csv"),
            "log_file": os.path.join(self.raw_dir, f"scraper_log_{self.date_str}.log"),
            "stats_json": os.path.join(self.raw_dir, f"scraper_stats_{self.date_str}.json")
        }
    
    def get_processar_outputs(self):
        """Get all output file paths for the processar_relevantes task"""
        return {
            "relevant_articles": os.path.join(self.structured_dir, f"artigos_google_municipios_pt_{self.date_iso}.csv"),
            "irrelevant_articles": os.path.join(self.structured_dir, f"artigos_irrelevantes_{self.date_iso}.csv"),
            "processing_log": os.path.join(self.structured_dir, f"processing_log_{self.date_str}.log"),
            "stats_json": os.path.join(self.structured_dir, f"processing_stats_{self.date_str}.json")
        }
    
    def get_filtrar_outputs(self):
        """Get all output file paths for the filtrar_vitimas task"""
        return {
            "victims_articles": os.path.join(self.processed_dir, f"artigos_vitimas_filtrados_{self.date_str}.csv"),
            "no_victims_articles": os.path.join(self.processed_dir, f"artigos_sem_vitimas_{self.date_str}.csv"),
            "filtering_log": os.path.join(self.processed_dir, f"filtering_log_{self.date_str}.log"),
            "stats_json": os.path.join(self.processed_dir, f"filtering_stats_{self.date_str}.json")
        }
    
    def get_export_outputs(self):
        """Get all output file paths for the export task"""
        return {
            "export_log": os.path.join(self.processed_dir, f"export_log_{self.date_str}.log"),
            "export_stats": os.path.join(self.processed_dir, f"export_stats_{self.date_str}.json"),
            "backup_csv": os.path.join(self.processed_dir, f"export_backup_{self.date_str}.csv")
        }
    
    def create_all_directories(self):
        """Create all necessary directories"""
        directories = [
            self.raw_dir,
            self.structured_dir,
            self.processed_dir
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
            
        return directories
    
    def get_all_paths_summary(self):
        """Get a summary of all paths for logging"""
        return {
            "base_dir": self.base_dir,
            "execution_date": self.date_iso,
            "raw_dir": self.raw_dir,
            "structured_dir": self.structured_dir,
            "processed_dir": self.processed_dir,
            "scraper_outputs": self.get_scraper_outputs(),
            "processar_outputs": self.get_processar_outputs(),
            "filtrar_outputs": self.get_filtrar_outputs(),
            "export_outputs": self.get_export_outputs()
        }

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
    """Run the Google News scraper with controlled output paths"""
    print("🚀 Starting Airflow-Optimized Google News Scraper")
    
    # Fix: Always use current date unless specific date is provided in DAG run config
    dag_run = context.get('dag_run')
    if dag_run and dag_run.conf:
        dias = dag_run.conf.get('dias', 1)
        date = dag_run.conf.get('date', '')
        max_execution_time = dag_run.conf.get('max_execution_time', 3600)
        use_current_date = dag_run.conf.get('use_current_date', True)  # Default to current date
    else:
        dias = 1
        date = ''
        max_execution_time = 3600
        use_current_date = True  # Default to current date
    
    # Initialize path manager with current date preference
    if date:
        # If specific date provided, use that
        target_date = datetime.strptime(date, '%Y-%m-%d')
        paths = GoogleScraperPaths(execution_date=target_date, use_current_date=False)
        print(f"📅 Using specific date from config: {date}")
    else:
        # Use current date by default
        paths = GoogleScraperPaths(use_current_date=True)
        target_date = paths.execution_date
        print(f"📅 Using current date: {target_date.strftime('%Y-%m-%d')}")
    
    # Create directories if they don't exist
    created_dirs = paths.create_all_directories()
    print(f"📁 Ensured directories exist: {created_dirs}")
    
    # Get scraper output paths
    scraper_outputs = paths.get_scraper_outputs()
    
    print(f"📅 Scraper Parameters:")
    print(f"  - Days: {dias}")
    print(f"  - Specific date: {date if date else 'None (use execution_date)'}")
    print(f"  - Max execution time: {max_execution_time} seconds")
    print(f"  - Target date: {paths.date_iso}")
    
    print(f"📁 Output paths:")
    for key, path in scraper_outputs.items():
        print(f"  - {key}: {path}")
    
    # Build command with controlled output paths
    cmd = [
        'python', '/opt/airflow/scripts/google_scraper/scraping/run_scraper_airflow.py',
        '--dias', str(dias),
        '--max_time', str(max_execution_time),
        '--output_dir', paths.raw_dir,
        '--date_str', paths.date_str_compact
    ]
    
    # Pass the actual target date to the command
    if date:
        cmd.extend(['--date', date])
    else:
        cmd.extend(['--date', target_date.strftime('%Y-%m-%d')])
    
    print(f"🔄 Running scraper command: {' '.join(cmd)}")
    
    # Set working directory and environment for real-time output
    env = os.environ.copy()
    env['PYTHONPATH'] = '/opt/airflow/scripts/google_scraper:' + env.get('PYTHONPATH', '')
    env['PYTHONUNBUFFERED'] = '1'
    
    # Run with real-time output capture
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd='/opt/airflow/scripts/google_scraper/scraping',
        env=env,
        bufsize=1
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
        print(f"⏰ Waiting for scraper completion with timeout: {max_execution_time} seconds")
        
        exit_code = process.wait(timeout=max_execution_time)
        stdout_thread.join()
        stderr_thread.join()
        
        stdout_content = ''.join(stdout_lines)
        stderr_content = ''.join(stderr_lines)
        
        if exit_code != 0:
            print(f"❌ Scraper failed with return code {exit_code}")
            if stderr_content:
                print(f"❌ Error details: {stderr_content}")
            raise Exception(f"Scraper failed with return code {exit_code}: {stderr_content}")
        
        print("✅ Scraper completed successfully!")
        
        # Verify outputs were created
        print("🔍 Verifying scraper outputs:")
        created_files = []
        for output_name, output_path in scraper_outputs.items():
            if os.path.exists(output_path):
                file_size = os.path.getsize(output_path)
                print(f"✅ {output_name}: {output_path} ({file_size} bytes)")
                created_files.append(output_path)
                
                # Log file content info for main output
                if output_name == 'final_csv':
                    try:
                        import pandas as pd
                        df = pd.read_csv(output_path)
                        print(f"📊 Number of articles scraped: {len(df)}")
                        if len(df) > 0:
                            print(f"📊 Columns: {list(df.columns)}")
                    except Exception as e:
                        print(f"⚠️ Could not read CSV details: {e}")
            else:
                print(f"⚠️ {output_name}: {output_path} (not found)")
        
        # Store output paths in XCom for next tasks
        context['task_instance'].xcom_push(key='scraper_outputs', value=scraper_outputs)
        context['task_instance'].xcom_push(key='created_files', value=created_files)
        context['task_instance'].xcom_push(key='paths_config', value=paths.get_all_paths_summary())
        
        return stdout_content
        
    except subprocess.TimeoutExpired:
        process.kill()
        print(f"⏰ Scraper timed out after {max_execution_time} seconds ({max_execution_time/60:.1f} minutes)")
        raise Exception(f"Scraper execution exceeded maximum time limit of {max_execution_time} seconds")
    except Exception as e:
        print(f"❌ Unexpected error in scraper task: {str(e)}")
        raise

def processar_relevantes_task(**context):
    """Process relevant articles with controlled output paths"""
    print("🔄 Starting processar_relevantes task")
    
    # Get parameters from DAG run configuration
    dag_run = context.get('dag_run')
    if dag_run and dag_run.conf:
        dias = dag_run.conf.get('dias', 1)
        date = dag_run.conf.get('date', '')
        use_current_date = dag_run.conf.get('use_current_date', True)
    else:
        dias = 1
        date = ''
        use_current_date = True
    
    # Get paths configuration from previous task or create new one
    try:
        paths_config = context['task_instance'].xcom_pull(key='paths_config', task_ids='executar_scraper_airflow')
        if paths_config and 'execution_date' in paths_config:
            print(f"📁 Using paths from scraper task: {paths_config['execution_date']}")
            target_date = datetime.strptime(paths_config['execution_date'], '%Y-%m-%d')
            paths = GoogleScraperPaths(execution_date=target_date, use_current_date=False)
        else:
            raise ValueError("No valid paths config from previous task")
    except (TypeError, KeyError, ValueError):
        # Fallback: use same date logic as scraper
        if date:
            target_date = datetime.strptime(date, '%Y-%m-%d')
            paths = GoogleScraperPaths(execution_date=target_date, use_current_date=False)
        else:
            paths = GoogleScraperPaths(use_current_date=True)
            target_date = paths.execution_date
    
    # Create directories if they don't exist
    created_dirs = paths.create_all_directories()
    print(f"📁 Ensured directories exist: {created_dirs}")
    
    # Get processar output paths
    processar_outputs = paths.get_processar_outputs()
    
    print(f"📅 Processar Parameters:")
    print(f"  - Days: {dias}")
    print(f"  - Specific date: {date if date else 'None (use execution_date)'}")
    print(f"  - Target date: {paths.date_iso}")
    print(f"📁 Input/Output paths:")
    
    # Find input file (output from scraper task)
    scraper_outputs = paths.get_scraper_outputs()
    input_files_to_check = [
        scraper_outputs['final_csv'],
        scraper_outputs['intermediate_csv']
    ]
    
    input_file = None
    for potential_input in input_files_to_check:
        print(f"🔍 Checking for input file: {potential_input}")
        if os.path.exists(potential_input):
            input_file = potential_input
            print(f"✅ Found input file: {input_file}")
            break
    
    if not input_file:
        print("❌ No input file found from scraper task")
        # Try legacy paths as fallback
        legacy_paths = [
            f"/opt/airflow/scripts/google_scraper/data/raw/{paths.year}/{paths.month}/{paths.day}/intermediate_google_news_{paths.date_str}.csv",
            "/opt/airflow/scripts/google_scraper/data/raw/intermediate_google_news.csv"
        ]
        
        for legacy_path in legacy_paths:
            print(f"🔍 Checking legacy path: {legacy_path}")
            if os.path.exists(legacy_path):
                input_file = legacy_path
                print(f"✅ Found legacy input file: {input_file}")
                break
        
        if not input_file:
            raise Exception(f"No input file found. Checked: {input_files_to_check + legacy_paths}")
    
    print(f"📁 Output paths:")
    for key, path in processar_outputs.items():
        print(f"  - {key}: {path}")
    
    cmd = [
        'python', '/opt/airflow/scripts/google_scraper/processador/processar_relevantes_airflow.py',
        '--dias', str(dias),
        '--input_file', input_file,
        '--output_dir', paths.structured_dir,
        '--date_str', paths.date_iso
    ]
    
    # Pass specific date
    if date:
        cmd.extend(['--date', date])
    else:
        cmd.extend(['--date', target_date.strftime('%Y-%m-%d')])
    
    print(f"🔄 Running processing command: {' '.join(cmd)}")
    
    # Set working directory and environment for real-time output
    env = os.environ.copy()
    env['PYTHONPATH'] = '/opt/airflow/scripts/google_scraper:' + env.get('PYTHONPATH', '')
    env['PYTHONUNBUFFERED'] = '1'
    
    # Run with real-time output capture
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd='/opt/airflow/scripts/google_scraper/processador',
        env=env,
        bufsize=1
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
        
        # Verify outputs were created
        print("🔍 Verifying processar outputs:")
        created_files = []
        for output_name, output_path in processar_outputs.items():
            if os.path.exists(output_path):
                file_size = os.path.getsize(output_path)
                print(f"✅ {output_name}: {output_path} ({file_size} bytes)")
                created_files.append(output_path)
                
                # Log file content info for main output
                if output_name == 'relevant_articles':
                    try:
                        import pandas as pd
                        df = pd.read_csv(output_path)
                        print(f"📊 Number of processed articles: {len(df)}")
                        if len(df) > 0:
                            print(f"📊 Columns: {list(df.columns)}")
                    except Exception as e:
                        print(f"⚠️ Could not read CSV details: {e}")
            else:
                print(f"⚠️ {output_name}: {output_path} (not found)")
        
        # Store output paths in XCom for next tasks
        context['task_instance'].xcom_push(key='processar_outputs', value=processar_outputs)
        context['task_instance'].xcom_push(key='created_files', value=created_files)
        context['task_instance'].xcom_push(key='paths_config', value=paths.get_all_paths_summary())
        
        return stdout_content
        
    except subprocess.TimeoutExpired:
        process.kill()
        print(f"⏰ Processing timed out after {timeout} seconds ({timeout/60:.1f} minutes)")
        raise Exception(f"Processing execution exceeded maximum time limit of {timeout} seconds")
    except Exception as e:
        print(f"❌ Unexpected error in processing task: {str(e)}")
        raise

def filtrar_vitimas_task(**context):
    """Filter articles with victim information using controlled output paths"""
    print("🔄 Starting filtrar_vitimas task")
    
    # Get parameters from DAG run configuration
    dag_run = context.get('dag_run')
    if dag_run and dag_run.conf:
        dias = dag_run.conf.get('dias', 1)
        date = dag_run.conf.get('date', '')
        use_current_date = dag_run.conf.get('use_current_date', True)
    else:
        dias = 1
        date = ''
        use_current_date = True
    
    # Get paths configuration from previous task or create new one
    try:
        paths_config = context['task_instance'].xcom_pull(key='paths_config', task_ids='processar_relevantes')
        if paths_config and 'execution_date' in paths_config:
            print(f"📁 Using paths from processar task: {paths_config['execution_date']}")
            target_date = datetime.strptime(paths_config['execution_date'], '%Y-%m-%d')
            paths = GoogleScraperPaths(execution_date=target_date, use_current_date=False)
        else:
            raise ValueError("No valid paths config from previous task")
    except (TypeError, KeyError, ValueError):
        # Fallback: use same date logic as previous tasks
        if date:
            target_date = datetime.strptime(date, '%Y-%m-%d')
            paths = GoogleScraperPaths(execution_date=target_date, use_current_date=False)
        else:
            paths = GoogleScraperPaths(use_current_date=True)
            target_date = paths.execution_date
    
    # Create directories if they don't exist
    created_dirs = paths.create_all_directories()
    print(f"📁 Ensured directories exist: {created_dirs}")
    
    # Get filtrar output paths
    filtrar_outputs = paths.get_filtrar_outputs()
    
    print(f"📅 Filtrar Parameters:")
    print(f"  - Days: {dias}")
    print(f"  - Specific date: {date if date else 'None (use execution_date)'}")
    print(f"  - Target date: {paths.date_iso}")
    print(f"📁 Input/Output paths:")
    
    # Find input file (output from processar task)
    processar_outputs = paths.get_processar_outputs()
    input_files_to_check = [
        processar_outputs['relevant_articles'],
        # Fallback to previous task outputs stored in XCom
    ]
    
    # Try to get input from XCom first
    try:
        processar_files = context['task_instance'].xcom_pull(key='created_files', task_ids='processar_relevantes')
        if processar_files:
            input_files_to_check.extend(processar_files)
    except:
        pass
    
    input_file = None
    for potential_input in input_files_to_check:
        print(f"🔍 Checking for input file: {potential_input}")
        if os.path.exists(potential_input):
            input_file = potential_input
            print(f"✅ Found input file: {input_file}")
            break
    
    if not input_file:
        print("❌ No input file found from processar task")
        # Try legacy paths as fallback
        legacy_paths = [
            f"/opt/airflow/scripts/google_scraper/data/structured/{paths.year}/{paths.month}/{paths.day}/artigos_google_municipios_pt_{paths.date_iso}.csv",
            f"/opt/airflow/scripts/google_scraper/data/raw/{paths.year}/{paths.month}/{paths.day}/artigos_google_municipios_pt_{paths.date_iso}.csv",
            "/opt/airflow/scripts/google_scraper/data/structured/artigos_google_municipios_pt.csv"
        ]
        
        for legacy_path in legacy_paths:
            print(f"🔍 Checking legacy path: {legacy_path}")
            if os.path.exists(legacy_path):
                input_file = legacy_path
                print(f"✅ Found legacy input file: {input_file}")
                break
        
        if not input_file:
            raise Exception(f"No input file found. Checked: {input_files_to_check + legacy_paths}")
    
    print(f"📁 Output paths:")
    for key, path in filtrar_outputs.items():
        print(f"  - {key}: {path}")
    
    cmd = [
        'python', '/opt/airflow/scripts/google_scraper/processador/filtrar_artigos_vitimas_airflow.py',
        '--dias', str(dias),
        '--input_file', input_file,
        '--output_dir', paths.processed_dir,
        '--date_str', paths.date_iso
    ]
    
    # Pass specific date
    if date:
        cmd.extend(['--date', date])
    else:
        cmd.extend(['--date', target_date.strftime('%Y-%m-%d')])
    
    print(f"🔄 Running filtering command: {' '.join(cmd)}")
    
    # Set working directory and environment for real-time output
    env = os.environ.copy()
    env['PYTHONPATH'] = '/opt/airflow/scripts/google_scraper:' + env.get('PYTHONPATH', '')
    env['PYTHONUNBUFFERED'] = '1'
    
    # Run with real-time output capture
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd='/opt/airflow/scripts/google_scraper/processador',
        env=env,
        bufsize=1
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
        
        # Verify outputs were created
        print("🔍 Verifying filtrar outputs:")
        created_files = []
        for output_name, output_path in filtrar_outputs.items():
            if os.path.exists(output_path):
                file_size = os.path.getsize(output_path)
                print(f"✅ {output_name}: {output_path} ({file_size} bytes)")
                created_files.append(output_path)
                
                # Log file content info for main output
                if output_name == 'victims_articles':
                    try:
                        import pandas as pd
                        df = pd.read_csv(output_path)
                        print(f"📊 Number of filtered articles with victims: {len(df)}")
                        if len(df) > 0:
                            print(f"📊 Columns: {list(df.columns)}")
                    except Exception as e:
                        print(f"⚠️ Could not read CSV details: {e}")
            else:
                print(f"⚠️ {output_name}: {output_path} (not found)")
        
        # Store output paths in XCom for next tasks
        context['task_instance'].xcom_push(key='filtrar_outputs', value=filtrar_outputs)
        context['task_instance'].xcom_push(key='created_files', value=created_files)
        context['task_instance'].xcom_push(key='paths_config', value=paths.get_all_paths_summary())
        
        return stdout_content
        
    except subprocess.TimeoutExpired:
        process.kill()
        print(f"⏰ Filtering timed out after {timeout} seconds ({timeout/60:.1f} minutes)")
        raise Exception(f"Filtering execution exceeded maximum time limit of {timeout} seconds")
    except Exception as e:
        print(f"❌ Unexpected error in filtering task: {str(e)}")
        raise

def export_supabase_task(**context):
    """Export filtered articles to Supabase using controlled output paths"""
    print("🔄 Starting export_supabase task")
    
    # Get parameters from DAG run configuration
    dag_run = context.get('dag_run')
    if dag_run and dag_run.conf:
        dias = dag_run.conf.get('dias', 1)
        date = dag_run.conf.get('date', '')
        use_current_date = dag_run.conf.get('use_current_date', True)
    else:
        dias = 1
        date = ''
        use_current_date = True
    
    # Get paths configuration from previous task or create new one
    try:
        paths_config = context['task_instance'].xcom_pull(key='paths_config', task_ids='filtrar_vitimas')
        if paths_config and 'execution_date' in paths_config:
            print(f"📁 Using paths from filtrar task: {paths_config['execution_date']}")
            target_date = datetime.strptime(paths_config['execution_date'], '%Y-%m-%d')
            paths = GoogleScraperPaths(execution_date=target_date, use_current_date=False)
        else:
            raise ValueError("No valid paths config from previous task")
    except (TypeError, KeyError, ValueError):
        # Fallback: use same date logic as previous tasks
        if date:
            target_date = datetime.strptime(date, '%Y-%m-%d')
            paths = GoogleScraperPaths(execution_date=target_date, use_current_date=False)
        else:
            paths = GoogleScraperPaths(use_current_date=True)
            target_date = paths.execution_date
    
    # Create directories if they don't exist
    created_dirs = paths.create_all_directories()
    print(f"📁 Ensured directories exist: {created_dirs}")
    
    # Get export output paths
    export_outputs = paths.get_export_outputs()
    
    # Generate expected table name for logging
    date_compact = paths.date_iso.replace('-', '')
    expected_table_name = f"artigos_filtrados_{date_compact}_staging"
    
    print(f"📅 Export Parameters:")
    print(f"  - Days: {dias}")
    print(f"  - Specific date: {date if date else 'None (use execution_date)'}")
    print(f"  - Target date: {paths.date_iso}")
    print(f"  - Database table: {expected_table_name}")
    print(f"📁 Input/Output paths:")
    
    # Find input file - prioritize files from filtrar task, but accept any relevant filtered file
    input_files_to_check = []
    
    # Get filtrar outputs first (both victims and non-victims files)
    filtrar_outputs = paths.get_filtrar_outputs()
    input_files_to_check.extend([
        filtrar_outputs['victims_articles'],  # Articles with victims (preferred)
        filtrar_outputs['no_victims_articles']  # Articles without victims but still relevant
    ])
    
    # Try to get input from XCom (all files created by filtrar task)
    try:
        filtrar_files = context['task_instance'].xcom_pull(key='created_files', task_ids='filtrar_vitimas')
        if filtrar_files:
            print(f"📋 Found {len(filtrar_files)} files from filtrar task via XCom")
            input_files_to_check.extend(filtrar_files)
    except:
        pass
    
    # Fallback to processar outputs if filtrar didn't produce anything
    processar_outputs = paths.get_processar_outputs()
    input_files_to_check.append(processar_outputs['relevant_articles'])
    
    # Try to get processar files from XCom as well
    try:
        processar_files = context['task_instance'].xcom_pull(key='created_files', task_ids='processar_relevantes')
        if processar_files:
            print(f"📋 Found {len(processar_files)} files from processar task via XCom")
            input_files_to_check.extend(processar_files)
    except:
        pass
    
    input_file = None
    input_file_info = []
    
    # Check all potential input files and collect info
    for potential_input in input_files_to_check:
        print(f"🔍 Checking for input file: {potential_input}")
        if os.path.exists(potential_input):
            file_size = os.path.getsize(potential_input)
            input_file_info.append((potential_input, file_size))
            print(f"✅ Found file: {potential_input} ({file_size} bytes)")
    
    # Select the best input file based on priority
    if input_file_info:
        # Sort by preference: vitimas_filtrados > sem_vitimas > municipios_pt > others
        def file_priority(file_path):
            if 'vitimas_filtrados' in file_path:
                return 1
            elif 'sem_vitimas' in file_path:
                return 2
            elif 'municipios_pt' in file_path:
                return 3
            else:
                return 4
        
        # Sort by priority, then by file size (larger files first)
        input_file_info.sort(key=lambda x: (file_priority(x[0]), -x[1]))
        input_file = input_file_info[0][0]
        
        print(f"📋 Selected input file: {input_file} ({input_file_info[0][1]} bytes)")
        
        # Log file type for clarity
        if 'vitimas_filtrados' in input_file:
            print("📊 Processing articles WITH victim information")
        elif 'sem_vitimas' in input_file:
            print("📊 Processing articles WITHOUT victims but disaster-related")
        elif 'municipios_pt' in input_file:
            print("📊 Processing all relevant disaster-related articles")
        else:
            print("📊 Processing filtered articles")
    
    if not input_file:
        print("⚠️ No input files found from previous tasks")
        # Try legacy paths as fallback
        legacy_paths = [
            f"/opt/airflow/scripts/google_scraper/data/processed/{paths.year}/{paths.month}/{paths.day}/artigos_vitimas_filtrados_{paths.date_str}.csv",
            f"/opt/airflow/scripts/google_scraper/data/processed/{paths.year}/{paths.month}/{paths.day}/artigos_sem_vitimas_{paths.date_str}.csv",
            f"/opt/airflow/scripts/google_scraper/data/structured/{paths.year}/{paths.month}/{paths.day}/artigos_google_municipios_pt_{paths.date_iso}.csv",
            "/opt/airflow/scripts/google_scraper/data/structured/artigos_vitimas_filtrados.csv"
        ]
        
        for legacy_path in legacy_paths:
            print(f"🔍 Checking legacy path: {legacy_path}")
            if os.path.exists(legacy_path):
                input_file = legacy_path
                print(f"✅ Found legacy input file: {input_file}")
                break
        
        if not input_file:
            print("ℹ️ No input files found - this may be normal if no disaster-related articles were detected")
            print("✅ Export task will proceed and handle empty data gracefully")
    
    print(f"📁 Output paths:")
    for key, path in export_outputs.items():
        print(f"  - {key}: {path}")
    
    cmd = [
        'python', '/opt/airflow/scripts/google_scraper/exportador_bd/export_to_supabase_airflow.py',
        '--output_dir', paths.processed_dir,
        '--date_str', paths.date_iso
    ]
    
    # Add input file if found
    if input_file:
        cmd.extend(['--input_file', input_file])
    
    # Pass specific date
    cmd.extend(['--date', target_date.strftime('%Y-%m-%d')])
    
    print(f"🔄 Running export command: {' '.join(cmd)}")
    
    # Set working directory and environment for real-time output
    env = os.environ.copy()
    env['PYTHONPATH'] = '/opt/airflow/scripts/google_scraper:' + env.get('PYTHONPATH', '')
    env['PYTHONUNBUFFERED'] = '1'
    
    # Run with real-time output capture
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
        
        # Verify outputs were created
        print("🔍 Verifying export outputs:")
        created_files = []
        for output_name, output_path in export_outputs.items():
            if os.path.exists(output_path):
                file_size = os.path.getsize(output_path)
                print(f"✅ {output_name}: {output_path} ({file_size} bytes)")
                created_files.append(output_path)
            else:
                print(f"⚠️ {output_name}: {output_path} (not found)")
        
        # Log export summary with table name
        if "export completed successfully" in stdout_content.lower():
            if "inserted/updated" in stdout_content:
                # Extract count from stdout
                import re
                match = re.search(r'inserted/updated (\d+)', stdout_content.lower())
                if match:
                    count = match.group(1)
                    print(f"📊 Successfully exported {count} articles to database table: {expected_table_name}")
                    print(f"🗄️ Database table: google_scraper.{expected_table_name}")
        
        # Store output paths in XCom for potential downstream tasks
        context['task_instance'].xcom_push(key='export_outputs', value=export_outputs)
        context['task_instance'].xcom_push(key='created_files', value=created_files)
        context['task_instance'].xcom_push(key='paths_config', value=paths.get_all_paths_summary())
        context['task_instance'].xcom_push(key='exported_table', value=expected_table_name)
        
        return stdout_content
        
    except subprocess.TimeoutExpired:
        process.kill()
        print(f"⏰ Export timed out after {timeout} seconds ({timeout/60:.1f} minutes)")
        raise Exception(f"Export execution exceeded maximum time limit of {timeout} seconds")
    except Exception as e:
        print(f"❌ Unexpected error in export task: {str(e)}")
        raise

# Create a task to check data directory and dependencies
verificar_directorio_dados = BashOperator(
    task_id='verificar_directorio_dados',
    bash_command='''
    
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

# Pipeline documentation
dag.doc_md = """
# SIMPREDE Google News Scraper Pipeline

## Pipeline Overview
This DAG implements a complete news scraping and processing pipeline for the SIMPREDE project, 
optimized for Airflow with controlled output paths and centralized path management.

**Date Handling**: By default, the pipeline uses the current date for file naming and processing.
You can override this by providing a specific date in the DAG run configuration.

## Configuration Parameters
- `dias`: Number of days to process (default: 1)
- `date`: Specific date to process (YYYY-MM-DD format, optional)
- `max_execution_time`: Maximum scraper execution time in seconds
- `use_current_date`: Whether to use current date instead of execution date (default: true)

## Date Configuration Examples

### Use Current Date (Default)
```json
{
  "dias": 1,
  "use_current_date": true
}
```

### Use Specific Date
```json
{
  "dias": 1,
  "date": "2025-01-15",
  "use_current_date": false
}
```

### Process Multiple Days from Today
```json
{
  "dias": 7,
  "use_current_date": true
}
```

## Pipeline Stages

### 1. **executar_scraper_airflow** 
- Scrapes Google News for disaster-related articles
- Uses current date by default for file naming
- Outputs: `intermediate_google_news_{current_date}.csv` and `google_news_articles_{current_date}.csv`
- Location: `/opt/airflow/scripts/google_scraper/data/raw/YYYY/MM/DD/`

### 2. **processar_relevantes**
- Processes scraped articles to extract relevant disaster information
- Inherits date from previous task for consistency
- Outputs: `artigos_google_municipios_pt_{date}.csv` and `artigos_irrelevantes_{date}.csv`
- Location: `/opt/airflow/scripts/google_scraper/data/structured/YYYY/MM/DD/`

### 3. **filtrar_vitimas**
- Applies comprehensive filtering to identify articles with victim information
- Maintains date consistency across pipeline
- Outputs: `artigos_vitimas_filtrados_{date}.csv` and `artigos_sem_vitimas_{date}.csv`
- Location: `/opt/airflow/scripts/google_scraper/data/processed/YYYY/MM/DD/`

### 4. **export_supabase**
- Exports ALL filtered articles to Supabase database (both with and without victims)
- Creates tables with naming convention: `artigos_filtrados_{YYYY-MM-DD}`
- Prioritizes articles with victims, but includes all disaster-related articles
- Handles cases where no disaster-related events were detected gracefully
- Outputs: Export logs, statistics, and backup CSV files
- Location: `/opt/airflow/scripts/google_scraper/data/processed/YYYY/MM/DD/`

## Export Logic
The export task intelligently selects input files in this priority order:
1. **Articles with victims** (`artigos_vitimas_filtrados_{date}.csv`) - highest priority
2. **Articles without victims** (`artigos_sem_vitimas_{date}.csv`) - disaster-related but no victims detected
3. **All relevant articles** (`artigos_google_municipios_pt_{date}.csv`) - fallback to all disaster-related articles

## Database Tables
- **Schema**: `google_scraper`
- **Table naming**: `artigos_filtrados_{YYYYMMDD}_staging` (e.g., `artigos_filtrados_20250605_staging`)
- **Content**: All disaster-related articles (with or without victims) from the filtering process
- **Primary key**: `ID` column (hash-based unique identifier)

This ensures that all disaster-related content is captured in clean, date-specific staging tables ready for further processing or promotion to production tables.

## Monitoring and Logging
- All tasks log progress and errors in real-time to Airflow logs.
- Key output files and directories are printed at each stage for verification.
- Export task provides a summary of inserted/updated records and the target database table.

## Troubleshooting Tips
- If no articles are found, verify the keywords and region settings in `keywords.json` and `municipios_por_distrito.json`.
- Check Airflow worker logs for detailed error messages and stack traces.
- Ensure network access to Google News and Supabase from the Airflow environment.
"""
