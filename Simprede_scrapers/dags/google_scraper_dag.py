# File: airflow-GOOGLE-NEWS-SCRAPER/dags/google_scraper_dag.py
# Script para o DAG do Airflow que executa o pipeline de scraping de notícias do Google
#!/usr/bin/env python3
"""
DAG do pipeline SIMPREDE para scraping e processamento de notícias do Google
Otimizado para produção com gestão eficiente de recursos e caminhos
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
    """
    Gestão centralizada e otimizada de caminhos para o pipeline
    Implementa padrões consistentes de organização de ficheiros
    """
    
    def __init__(self, base_dir="/opt/airflow", execution_date=None, use_current_date=False):
        self.base_dir = base_dir
        
        # Determina data de execução
        if use_current_date or execution_date is None:
            self.execution_date = datetime.now()
        else:
            self.execution_date = execution_date
        
        # Inicializa estruturas de caminhos
        self._init_base_paths()
        self._init_date_structure()
        self._init_data_directories()
    
    def _init_base_paths(self):
        """Inicializa caminhos base do sistema"""
        self.scripts_dir = os.path.join(self.base_dir, "scripts", "google_scraper")
        self.data_dir = os.path.join(self.base_dir, "data")
    
    def _init_date_structure(self):
        """Inicializa estrutura baseada em data"""
        self.year = self.execution_date.strftime("%Y")
        self.month = self.execution_date.strftime("%m")
        self.day = self.execution_date.strftime("%d")
        self.date_str = self.execution_date.strftime("%Y%m%d")
        self.date_iso = self.execution_date.strftime("%Y-%m-%d")
    
    def _init_data_directories(self):
        """Inicializa directorias de dados organizadas"""
        self.raw_dir = os.path.join(self.data_dir, "raw", self.year, self.month, self.day)
        self.structured_dir = os.path.join(self.data_dir, "structured", self.year, self.month, self.day)
        self.processed_dir = os.path.join(self.data_dir, "processed", self.year, self.month, self.day)
    
    def get_output_paths(self):
        """
        Retorna todos os caminhos de saída organizados por etapa
        Facilita gestão e verificação de ficheiros
        """
        return {
            "scraper": {
                "intermediate": os.path.join(self.raw_dir, f"intermediate_google_news_{self.date_str}.csv"),
                "final": os.path.join(self.raw_dir, f"google_news_articles_{self.date_str}.csv"),
                "log": os.path.join(self.raw_dir, f"scraper_log_{self.date_str}.log")
            },
            "processar": {
                "articles": os.path.join(self.structured_dir, f"artigos_google_municipios_pt_{self.date_iso}.csv"),
                "log": os.path.join(self.structured_dir, f"processing_log_{self.date_str}.log")
            },
            "filtrar": {
                "victims": os.path.join(self.processed_dir, f"artigos_vitimas_filtrados_{self.date_str}.csv"),
                "no_victims": os.path.join(self.processed_dir, f"artigos_sem_vitimas_{self.date_str}.csv"),
                "log": os.path.join(self.processed_dir, f"filtering_log_{self.date_str}.log")
            },
            "export": {
                "backup": os.path.join(self.processed_dir, f"export_backup_{self.date_str}.csv"),
                "log": os.path.join(self.processed_dir, f"export_log_{self.date_str}.log")
            }
        }
    
    def create_directories(self):
        """Cria todas as directorias necessárias de forma eficiente"""
        directories = [self.raw_dir, self.structured_dir, self.processed_dir]
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
        return directories

class TaskExecutor:
    """
    Executor otimizado para tarefas do pipeline
    Implementa logging em tempo real e gestão de timeouts
    """
    
    @staticmethod
    def execute_with_logging(cmd, cwd, timeout, task_name):
        """
        Executa comando com logging em tempo real e controlo de timeout
        Melhora visibilidade no Airflow UI
        """
        print(f"🔄 A executar {task_name}: {' '.join(cmd)}")
        
        # Configura ambiente para output não bufferizado
        env = os.environ.copy()
        env['PYTHONPATH'] = '/opt/airflow/scripts/google_scraper:' + env.get('PYTHONPATH', '')
        env['PYTHONUNBUFFERED'] = '1'
        
        # Inicia processo
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=cwd,
            env=env,
            bufsize=1
        )
        
        stdout_lines = []
        stderr_lines = []
        
        def read_output(pipe, lines_list, prefix):
            """Lê output em tempo real"""
            for line in iter(pipe.readline, ''):
                if line:
                    print(f"{prefix} {line.rstrip()}")
                    lines_list.append(line)
            pipe.close()
        
        # Cria threads para captura de output
        stdout_thread = threading.Thread(
            target=read_output, 
            args=(process.stdout, stdout_lines, "📋")
        )
        stderr_thread = threading.Thread(
            target=read_output, 
            args=(process.stderr, stderr_lines, "⚠️")
        )
        
        stdout_thread.start()
        stderr_thread.start()
        
        try:
            exit_code = process.wait(timeout=timeout)
            stdout_thread.join()
            stderr_thread.join()
            
            stdout_content = ''.join(stdout_lines)
            stderr_content = ''.join(stderr_lines)
            
            if exit_code != 0:
                error_msg = f"{task_name} falhou com código {exit_code}"
                if stderr_content:
                    error_msg += f": {stderr_content}"
                raise Exception(error_msg)
            
            print(f"✅ {task_name} concluído com sucesso!")
            return stdout_content
            
        except subprocess.TimeoutExpired:
            process.kill()
            timeout_msg = f"{task_name} excedeu timeout de {timeout} segundos"
            print(f"⏰ {timeout_msg}")
            raise Exception(timeout_msg)

# Configuração do DAG
default_args = {
    'owner': 'simprede',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'do_xcom_push': False,
}

dag = DAG(
    'pipeline_scraper_google_optimized',
    default_args=default_args,
    description='Pipeline otimizado de scraping e processamento de notícias do Google',
    schedule="@daily",
    start_date=datetime(2025, 6, 1),
    catchup=False,
    tags=['google', 'scraping', 'noticias', 'simprede', 'otimizado'],
    max_active_runs=1,
    doc_md=__doc__,
)

def get_execution_config(context):
    """
    Obtém configuração de execução de forma centralizada
    Permite customização via DAG run configuration
    """
    dag_run = context.get('dag_run')
    default_config = {
        'dias': 1,
        'date': '',
        'use_current_date': True,
        'max_execution_time': 3600,
        'ml_threshold': 0.6,
        'use_ml_filtering': True
    }
    
    if dag_run and dag_run.conf:
        default_config.update(dag_run.conf)
    
    # Determina data de execução
    if default_config['date']:
        target_date = datetime.strptime(default_config['date'], '%Y-%m-%d')
        paths = GoogleScraperPaths(execution_date=target_date, use_current_date=False)
    else:
        paths = GoogleScraperPaths(use_current_date=True)
        target_date = paths.execution_date
    
    return default_config, paths, target_date

def executar_scraper(**context):
    """Executa scraper do Google News de forma otimizada"""
    config, paths, target_date = get_execution_config(context)
    paths.create_directories()
    outputs = paths.get_output_paths()['scraper']
    
    cmd = [
        'python', '/opt/airflow/scripts/google_scraper/scraping/run_scraper_airflow.py',
        '--dias', str(config['dias']),
        '--max_time', str(config['max_execution_time']),
        '--output_dir', paths.raw_dir,
        '--date_str', paths.date_str,
        '--date', target_date.strftime('%Y-%m-%d')
    ]
    
    result = TaskExecutor.execute_with_logging(
        cmd, '/opt/airflow/scripts/google_scraper/scraping', 
        config['max_execution_time'], 'Scraper'
    )
    
    # Verifica outputs criados
    created_files = []
    for name, path in outputs.items():
        if os.path.exists(path):
            created_files.append(path)
            print(f"✅ {name}: {path}")
    
    context['task_instance'].xcom_push(key='created_files', value=created_files)
    context['task_instance'].xcom_push(key='paths_config', value=paths.get_output_paths())
    
    return result

def processar_relevantes(**context):
    """Processa artigos relevantes com otimizações"""
    config, paths, target_date = get_execution_config(context)
    
    # Encontra ficheiro de entrada de forma inteligente
    scraper_files = context['task_instance'].xcom_pull(
        key='created_files', task_ids='executar_scraper'
    ) or []
    
    input_file = None
    for file_path in scraper_files:
        if os.path.exists(file_path) and 'google_news_articles' in file_path:
            input_file = file_path
            break
    
    if not input_file:
        # Fallback para caminhos esperados
        outputs = paths.get_output_paths()['scraper']
        for path in [outputs['final'], outputs['intermediate']]:
            if os.path.exists(path):
                input_file = path
                break
    
    if not input_file:
        raise Exception("Ficheiro de entrada do scraper não encontrado")
    
    cmd = [
        'python', '/opt/airflow/scripts/google_scraper/processador/processar_relevantes_airflow.py',
        '--dias', str(config['dias']),
        '--input_file', input_file,
        '--output_dir', paths.structured_dir,
        '--date_str', paths.date_iso,
        '--date', target_date.strftime('%Y-%m-%d')
    ]
    
    result = TaskExecutor.execute_with_logging(
        cmd, '/opt/airflow/scripts/google_scraper/processador', 
        1800, 'Processamento'
    )
    
    # Verifica e armazena outputs
    outputs = paths.get_output_paths()['processar']
    created_files = [path for path in outputs.values() if os.path.exists(path)]
    
    context['task_instance'].xcom_push(key='created_files', value=created_files)
    return result

def filtrar_vitimas_task(**context):
    """Filter articles with victim information using controlled output paths"""
    print("🔄 Starting filtrar_vitimas task")
    
    # Obter configuração da tarefa e gestão de caminhos
    config, paths, target_date = get_execution_config(context)
    dias = config['dias']
    max_execution_time = config['max_execution_time']
    
    # Create directories if they don't exist
    created_dirs = paths.create_directories()
    print(f"📁 Ensured directories exist: {created_dirs}")
    
    # Get filtrar output paths
    filtrar_outputs = paths.get_output_paths()["filtrar"]
    
    print(f"📅 Filtrar Parameters:")
    print(f"  - Days: {dias}")
    print(f"  - Specific date: {config['date'] if config['date'] else 'None (use execution_date)'}")
    print(f"  - Target date: {paths.date_iso}")
    print(f"📁 Input/Output paths:")
    
    # Enhanced input file search - check multiple patterns and locations
    current_date = target_date.strftime("%Y%m%d")
    current_year = target_date.strftime("%Y")
    current_month = target_date.strftime("%m")
    current_day = target_date.strftime("%d")
    date_suffix = target_date.strftime("%Y-%m-%d")
    
    # Comprehensive list of possible input file locations
    input_files_to_check = [
        # Primary expected outputs from processar_relevantes_airflow
        f"/opt/airflow/data/structured/{current_year}/{current_month}/{current_day}/artigos_google_municipios_pt_{date_suffix}.csv",
        f"/opt/airflow/data/raw/{current_year}/{current_month}/{current_day}/artigos_google_municipios_pt_{date_suffix}.csv",
        
        # Alternative date formats
        f"/opt/airflow/data/structured/{current_year}/{current_month}/{current_day}/artigos_google_municipios_pt_{current_date}.csv",
        f"/opt/airflow/data/raw/{current_year}/{current_month}/{current_day}/artigos_google_municipios_pt_{current_date}.csv",
        
        # Files without date suffix in organized structure
        f"/opt/airflow/data/structured/{current_year}/{current_month}/{current_day}/artigos_google_municipios_pt.csv",
        f"/opt/airflow/data/raw/{current_year}/{current_month}/{current_day}/artigos_google_municipios_pt.csv",
        
        # Legacy structured directory
        f"/opt/airflow/data/structured/artigos_google_municipios_pt_{date_suffix}.csv",
        f"/opt/airflow/data/structured/artigos_google_municipios_pt_{current_date}.csv",
        f"/opt/airflow/data/structured/artigos_google_municipios_pt.csv",
        
        # Raw directory fallbacks
        f"/opt/airflow/data/raw/artigos_google_municipios_pt_{date_suffix}.csv",
        f"/opt/airflow/data/raw/artigos_google_municipios_pt_{current_date}.csv",
        f"/opt/airflow/data/raw/artigos_google_municipios_pt.csv"
    ]
    
    # Try to get input from XCom first
    processar_files = []
    try:
        processar_files = context['task_instance'].xcom_pull(
            key='created_files', task_ids='processar_relevantes'
        ) or []
        print(f"📋 XCom files from processar_relevantes: {processar_files}")
        
        # Add XCom files to the search list
        for file_path in processar_files:
            if file_path and os.path.exists(file_path):
                input_files_to_check.insert(0, file_path)  # Add to front of list
        
    except Exception as e:
        print(f"⚠️ Could not get XCom data: {e}")
    
    # Check if processar_relevantes task actually completed successfully
    try:
        processar_state = context['task_instance'].get_dagrun().get_task_instance('processar_relevantes').current_state()
        print(f"📊 Processar_relevantes task state: {processar_state}")
    except Exception as e:
        print(f"⚠️ Could not check processar_relevantes state: {e}")
    
    # Find the first existing input file
    input_file = None
    input_file_size = 0
    print(f"🔍 Searching for input file in {len(input_files_to_check)} locations:")
    
    for i, potential_input in enumerate(input_files_to_check):
        print(f"   {i+1:2d}. Checking: {potential_input}")
        if potential_input and os.path.exists(potential_input):
            file_size = os.path.getsize(potential_input)
            print(f"✅ Found input file: {potential_input} ({file_size} bytes)")
            input_file = potential_input
            input_file_size = file_size
            break
    
    # Handle the case where no input file exists (no articles were processed)
    if not input_file:
        print(f"❌ No input file found from processar task")
        
        # List what files actually exist in the expected directories
        check_dirs = [
            f"/opt/airflow/data/structured/{current_year}/{current_month}/{current_day}",
            f"/opt/airflow/data/raw/{current_year}/{current_month}/{current_day}",
            "/opt/airflow/data/structured",
            "/opt/airflow/data/raw"
        ]
        
        has_any_files = False
        for check_dir in check_dirs:
            if os.path.exists(check_dir):
                try:
                    files = os.listdir(check_dir)
                    if files:
                        has_any_files = True
                        print(f"📁 Files in {check_dir}: {files}")
                    else:
                        print(f"📁 Directory exists but is empty: {check_dir}")
                except Exception as e:
                    print(f"❌ Could not list directory {check_dir}: {e}")
            else:
                print(f"📁 Directory does not exist: {check_dir}")
        
        if not has_any_files:
            print("ℹ️ No files found in any expected directories")
            print("📊 This suggests either:")
            print("   1. The scraper found no articles for the specified date/criteria")
            print("   2. No articles passed the relevance filtering in processar_relevantes")
            print("   3. All articles were filtered out as not disaster-related")
            
            # Create empty output files to indicate successful completion with no data
            print("📝 Creating empty output files to indicate successful completion with no data")
            
            # Create empty CSV files with headers
            import csv
            
            # Create empty victims file
            with open(filtrar_outputs['victims'], 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['id', 'titulo', 'url', 'data_publicacao', 'fonte', 'localidade', 
                               'categoria', 'subcategoria', 'vitimas', 'texto_completo', 'processed_date'])
            
            # Create empty no_victims file
            with open(filtrar_outputs['no_victims'], 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['id', 'titulo', 'url', 'data_publicacao', 'fonte', 'localidade', 
                               'categoria', 'subcategoria', 'vitimas', 'texto_completo', 'processed_date'])
            
            # Create log file
            with open(filtrar_outputs['log'], 'w', encoding='utf-8') as f:
                f.write(f"Filtrar task completed at {datetime.now().isoformat()}\n")
                f.write(f"Target date: {paths.date_iso}\n")
                f.write("Status: No input files found - no articles to process\n")
                f.write("This indicates no disaster-related articles were found for this date.\n")
            
            print("✅ Empty output files created successfully")
            
            # Store results in XCom
            created_files = [filtrar_outputs['victims'], filtrar_outputs['no_victims'], filtrar_outputs['log']]
            context['task_instance'].xcom_push(key='created_files', value=created_files)
            context['task_instance'].xcom_push(key='processing_status', value='no_input_data')
            
            return "✅ Filtrar task completed successfully - no articles to process"
        
        # If there are files but not the expected ones, still raise an error
        checked_paths = input_files_to_check[:4]  # Show first 4 for brevity
        raise Exception(f"No input file found. Checked: {checked_paths}")
    
    # Check if input file is empty or very small
    if input_file_size < 100:  # Less than 100 bytes suggests empty or header-only file
        print(f"⚠️ Input file is very small ({input_file_size} bytes) - likely contains no data")
        print("📊 This suggests no relevant articles were found by processar_relevantes")
        
        # Still proceed but note this in the logs
        print("🔄 Proceeding with filtering despite small input file size")
    
    print(f"📁 Output paths:")
    print(f"  - victims: {filtrar_outputs['victims']}")
    print(f"  - no_victims: {filtrar_outputs['no_victims']}")
    print(f"  - log: {filtrar_outputs['log']}")
    
    # Build the filtering command with the found input file
    cmd = [
        'python', '/opt/airflow/scripts/google_scraper/processador/filtrar_artigos_vitimas_airflow.py',
        '--dias', str(dias),
        '--input_file', input_file,
        '--output_dir', paths.processed_dir,
        '--date_str', paths.date_iso,
        '--ml_threshold', str(config['ml_threshold']),
        '--use_ml_filtering', str(config['use_ml_filtering']).lower(),
        '--date', target_date.strftime('%Y-%m-%d')
    ]
    
    print(f"🔄 Running filtering command: {' '.join(cmd)}")
    
    try:
        result = TaskExecutor.execute_with_logging(
            cmd, '/opt/airflow/scripts/google_scraper/processador', 
            max_execution_time, 'Filtrar'
        )
    except Exception as e:
        print(f"❌ Filtering command failed: {str(e)}")
        # Check if this is due to empty input
        if "empty" in str(e).lower() or "no data" in str(e).lower():
            print("ℹ️ Filtering failed due to empty input - this is expected when no articles are found")
            
            # Create empty output files as fallback
            import csv
            with open(filtrar_outputs['victims'], 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['id', 'titulo', 'url', 'data_publicacao', 'fonte', 'localidade', 
                               'categoria', 'subcategoria', 'vitimas', 'texto_completo', 'processed_date'])
            
            with open(filtrar_outputs['no_victims'], 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['id', 'titulo', 'url', 'data_publicacao', 'fonte', 'localidade', 
                               'categoria', 'subcategoria', 'vitimas', 'texto_completo', 'processed_date'])
            
            # Store results and return success
            created_files = [filtrar_outputs['victims'], filtrar_outputs['no_victims']]
            context['task_instance'].xcom_push(key='created_files', value=created_files)
            context['task_instance'].xcom_push(key='processing_status', value='empty_input_handled')
            
            return "✅ Filtrar task completed - handled empty input gracefully"
        else:
            raise  # Re-raise if it's a different error
    
    # Verify outputs were created
    print(f"🔍 Verifying filtrar outputs:")
    created_files = []
    for name, path in filtrar_outputs.items():
        if os.path.exists(path):
            file_size = os.path.getsize(path)
            print(f"✅ {name}: {path} ({file_size} bytes)")
            created_files.append(path)
        else:
            print(f"⚠️ {name}: {path} (not found)")
    
    # Store results in XCom
    context['task_instance'].xcom_push(key='created_files', value=created_files)
    context['task_instance'].xcom_push(key='processing_status', value='completed_with_data')
    
    return result

def export_supabase_task(**context):
    """Export filtered articles to Supabase using controlled output paths"""
    print("🔄 Starting export_supabase task")
    
    # Obter configuração da tarefa e gestão de caminhos
    config, paths, target_date = get_execution_config(context)
    dias = config['dias']
    max_execution_time = config['max_execution_time']
    
    # Create directories if they don't exist
    created_dirs = paths.create_directories()
    print(f"📁 Ensured directories exist: {created_dirs}")
    
    # Get export output paths
    export_outputs = paths.get_output_paths()["export"]
    
    # Generate expected table name for logging
    date_compact = paths.date_iso.replace('-', '')
    expected_table_name = f"artigos_filtrados_{date_compact}_staging"
    
    print(f"📅 Export Parameters:")
    print(f"  - Days: {dias}")
    print(f"  - Specific date: {config['date'] if config['date'] else 'None (use execution_date)'}")
    print(f"  - Target date: {paths.date_iso}")
    print(f"  - Database table: {expected_table_name}")
    print(f"📁 Input/Output paths:")
    
    # Find input file - prioritize files from filtrar task, but accept any relevant filtered file
    input_files_to_check = []
    
    # Get filtrar outputs first (both victims and non-victims files)
    filtrar_outputs = paths.get_output_paths()["filtrar"]
    input_files_to_check.extend([
        filtrar_outputs['victims'],  # Corrigido: usar 'victims' em vez de 'victims_articles'
        filtrar_outputs['no_victims']  # Corrigido: usar 'no_victims' em vez de 'no_victims_articles'
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
    processar_outputs = paths.get_output_paths()["processar"]
    input_files_to_check.append(processar_outputs['articles'])  # Corrigido: usar 'articles' em vez de 'relevant_articles'
    
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
        print(f"🔍 Checking for input file: {potential_input}")  # Corrigido: removido 'potentialial_input'
        if os.path.exists(potential_input):
            file_size = os.path.getsize(potential_input)
            input_file_info.append((potential_input, file_size))
            print(f"✅ Found file: {potential_input} ({file_size} bytes)")  # Corrigido: removido 'potentialial_input'
    
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
    
    stdout_lines = []
    stderr_lines = []
    
    def read_pipe(pipe, lines_list, prefix):
        for line in iter(pipe.readline, ''):
            if line:
                print(f"{prefix} {line.rstrip()}")
                lines_list.append(line)
        pipe.close()
    
    # Cria threads para stdout e stderr
    stdout_thread = threading.Thread(target=read_pipe, args=(process.stdout, stdout_lines, "📋"))
    stderr_thread = threading.Thread(target=read_pipe, args=(process.stderr, stderr_lines, "⚠️"))
    
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
        context['task_instance'].xcom_push(key='paths_config', value=paths.get_output_paths())
        context['task_instance'].xcom_push(key='exported_table', value=expected_table_name)
        
        return stdout_content
        
    except subprocess.TimeoutExpired:
        process.kill()
        print(f"⏰ Export timed out after {timeout} seconds ({timeout/60:.1f} minutes)")
        raise Exception(f"Export execution exceeded maximum time limit of {timeout} seconds")
    except Exception as e:
        print(f"❌ Unexpected error in export task: {str(e)}")
        raise

def export_to_gcs_task(**context):
    """Export all pipeline files to Google Cloud Storage maintaining directory structure"""
    print("🔄 Starting export_to_gcs task")
    
    # Obter configuração da tarefa e gestão de caminhos
    config, paths, target_date = get_execution_config(context)
    dias = config['dias']
    max_execution_time = config['max_execution_time']
    
    # Create directories if they don't exist
    created_dirs = paths.create_directories()
    print(f"📁 Ensured directories exist: {created_dirs}")
    
    print(f"📅 GCS Export Parameters:")
    print(f"  - Days: {dias}")
    print(f"  - Specific date: {config['date'] if config['date'] else 'None (use execution_date)'}")
    print(f"  - Target date: {paths.date_iso}")
    print(f"  - Base data directory: {paths.data_dir}")
    print(f"  - GCS Project ID: {config.get('gcs_project_id') or 'From config/environment'}")
    print(f"  - GCS Bucket: {config.get('gcs_bucket_name') or 'From config/environment'}")
    
    # Build command for GCS export
    cmd = [
        'python', '/opt/airflow/scripts/google_scraper/exportador_gcs/export_to_gcs_airflow.py',
        '--date', target_date.strftime('%Y-%m-%d'),
        '--base_data_dir', paths.data_dir,
        '--output_dir', paths.processed_dir
    ]
    
    # Add GCS configuration if provided
    if config.get('gcs_project_id'):
        cmd.extend(['--gcs_project_id', config['gcs_project_id']])
    if config.get('gcs_bucket_name'):
        cmd.extend(['--gcs_bucket_name', config['gcs_bucket_name']])
    
    print(f"🔄 Running GCS export command: {' '.join(cmd)}")
    
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
        cwd='/opt/airflow/scripts/google_scraper/exportador_gcs',
        env=env,
        bufsize=1
    )
    
    stdout_lines = []
    stderr_lines = []
    
    def read_pipe(pipe, lines_list, prefix):
        for line in iter(pipe.readline, ''):
            if line:
                print(f"{prefix} {line.rstrip()}")
                lines_list.append(line)
        pipe.close()
    
    # Cria threads para stdout e stderr
    stdout_thread = threading.Thread(target=read_pipe, args=(process.stdout, stdout_lines, "📋"))
    stderr_thread = threading.Thread(target=read_pipe, args=(process.stderr, stderr_lines, "⚠️"))
    
    # Start threads and wait for them to finish
    stdout_thread.start()
    stderr_thread.start()
    
    # Wait for process to complete with timeout
    try:
        timeout = 900  # 15 minutes timeout for GCS upload
        print(f"⏰ Waiting for GCS export completion with timeout: {timeout} seconds")
        
        exit_code = process.wait(timeout=timeout)
        stdout_thread.join()
        stderr_thread.join()
        
        stdout_content = ''.join(stdout_lines)
        stderr_content = ''.join(stderr_lines)
        
        if exit_code != 0:
            print(f"❌ GCS export failed with return code {exit_code}")
            if stderr_content:
                print(f"❌ Error details: {stderr_content}")
            raise Exception(f"GCS export failed with return code {exit_code}: {stderr_content}")
        
        print("✅ GCS export completed successfully!")
        
        # Log export summary
        if "export completed successfully" in stdout_content.lower():
            # Extract statistics from stdout
            import re
            files_match = re.search(r'(\d+) files uploaded', stdout_content)
            size_match = re.search(r'(\d+\.?\d*) MB', stdout_content)
            bucket_match = re.search(r'gs://([^\s]+)', stdout_content)
            
            if files_match:
                files_count = files_match.group(1)
                print(f"📊 Successfully uploaded {files_count} files to GCS")
            
            if size_match:
                size_mb = size_match.group(1)
                print(f"📊 Total size uploaded: {size_mb} MB")
            
            if bucket_match:
                bucket_name = bucket_match.group(1)
                print(f"☁️ GCS bucket: gs://{bucket_name}")
        
        # Store output information in XCom
        gcs_export_info = {
            "target_date": paths.date_iso,
            "base_data_dir": paths.data_dir,
            "export_status": "success",
            "export_log": stdout_content[:1000]  # Truncate for XCom storage
        }
        
        context['task_instance'].xcom_push(key='gcs_export_info', value=gcs_export_info)
        context['task_instance'].xcom_push(key='paths_config', value=paths.get_output_paths())
        
        return stdout_content
        
    except subprocess.TimeoutExpired:
        process.kill()
        print(f"⏰ GCS export timed out after {timeout} seconds ({timeout/60:.1f} minutes)")
        raise Exception(f"GCS export execution exceeded maximum time limit of {timeout} seconds")
    except Exception as e:
        print(f"❌ Unexpected error in GCS export task: {str(e)}")
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
    python_callable=executar_scraper,
    dag=dag,
)

processar_relevantes = PythonOperator(
    task_id='processar_relevantes',
    python_callable=processar_relevantes,
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

exportar_para_gcs = PythonOperator(
    task_id='exportar_para_gcs',
    python_callable=export_to_gcs_task,
    dag=dag,
)

# Set dependencies
verificar_directorio_dados >> executar_scraper >> processar_relevantes >> filtrar_artigos_vitimas >> exportar_para_supabase >> exportar_para_gcs


def find_input_file(context, task_ids, output_keys, paths, stage_name):
    """
    Enhanced function to find input files with comprehensive search
    """
    input_files = []
    
    # Try to get files from XCom first
    for task_id in task_ids if isinstance(task_ids, list) else [task_ids]:
        try:
            xcom_files = context['task_instance'].xcom_pull(
                key='created_files', task_ids=task_id
            ) or []
            input_files.extend(xcom_files)
            print(f"📋 XCom files from {task_id}: {xcom_files}")
        except Exception as e:
            print(f"⚠️ Could not get XCom from {task_id}: {e}")
    
    # Add expected file paths based on stage
    expected_paths = []
    target_date = paths.execution_date
    current_date = target_date.strftime("%Y%m%d")
    current_year = target_date.strftime("%Y")
    current_month = target_date.strftime("%m")
    current_day = target_date.strftime("%d")
    date_suffix = target_date.strftime("%Y-%m-%d")
    
    if stage_name == "processar":
        # Files from scraper stage
        expected_paths = [
            f"/opt/airflow/data/raw/{current_year}/{current_month}/{current_day}/intermediate_google_news_{current_date}.csv",
            f"/opt/airflow/data/raw/intermediate_google_news_{current_date}.csv",
            f"/opt/airflow/data/raw/intermediate_google_news.csv"
        ]
    elif stage_name == "filtrar":
        # Files from processar stage
        expected_paths = [
            f"/opt/airflow/data/structured/{current_year}/{current_month}/{current_day}/artigos_google_municipios_pt_{date_suffix}.csv",
            f"/opt/airflow/data/raw/{current_year}/{current_month}/{current_day}/artigos_google_municipios_pt_{date_suffix}.csv",
            f"/opt/airflow/data/structured/{current_year}/{current_month}/{current_day}/artigos_google_municipios_pt_{current_date}.csv",
            f"/opt/airflow/data/raw/{current_year}/{current_month}/{current_day}/artigos_google_municipios_pt_{current_date}.csv",
            f"/opt/airflow/data/structured/{current_year}/{current_month}/{current_day}/artigos_google_municipios_pt.csv",
            f"/opt/airflow/data/raw/{current_year}/{current_month}/{current_day}/artigos_google_municipios_pt.csv",
            f"/opt/airflow/data/structured/artigos_google_municipios_pt_{date_suffix}.csv",
            f"/opt/airflow/data/structured/artigos_google_municipios_pt_{current_date}.csv",
            f"/opt/airflow/data/structured/artigos_google_municipios_pt.csv"
        ]
    
    # Combine XCom and expected paths
    all_possible_paths = input_files + expected_paths
    
    # Remove duplicates while preserving order
    seen = set()
    unique_paths = []
    for path in all_possible_paths:
        if path and path not in seen:
            unique_paths.append(path)
            seen.add(path)
    
    print(f"🔍 Searching for {stage_name} input file in {len(unique_paths)} locations:")
    
    # Find first existing file
    for i, file_path in enumerate(unique_paths):
        print(f"   {i+1:2d}. Checking: {file_path}")
        if file_path and os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            print(f"✅ Found input file: {file_path} ({file_size} bytes)")
            return file_path
    
    print(f"❌ No input file found for {stage_name}")
    
    # List actual files in directories for debugging
    check_dirs = [
        f"/opt/airflow/data/structured/{current_year}/{current_month}/{current_day}",
        f"/opt/airflow/data/raw/{current_year}/{current_month}/{current_day}",
        "/opt/airflow/data/structured",
        "/opt/airflow/data/raw"
    ]
    
    for check_dir in check_dirs:
        if os.path.exists(check_dir):
            try:
                files = [f for f in os.listdir(check_dir) if f.endswith('.csv')]
                if files:
                    print(f"📁 CSV files in {check_dir}: {files}")
            except Exception as e:
                print(f"❌ Could not list directory {check_dir}: {e}")
    
    return None
