# File: airflow-GOOGLE-NEWS-SCRAPER/scripts/google_scraper/exportador_gcs/export_to_gcs_airflow.py
# Script para exportar arquivos do pipeline SIMPREDE para o Google Cloud Storage (GCS)
# This script é executado como parte do DAG do Airflow e deve ser compatível com o ambiente do Airflow
#!/usr/bin/env python3
"""
SIMPREDE Pipeline - Export to Google Cloud Storage
Exports all pipeline files maintaining directory structure to GCS bucket
"""

import os
import sys
import json
import argparse
import logging
import traceback
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path
import shutil
from dotenv import load_dotenv, find_dotenv

# Add the google_scraper directory to Python path for imports
sys.path.insert(0, '/opt/airflow/scripts/google_scraper')

# Load environment variables from the project root
load_dotenv(find_dotenv())

try:
    from google.cloud import storage
    from google.cloud.exceptions import NotFound, Conflict
except ImportError as e:
    print(f"❌ Google Cloud Storage library not found: {e}")
    print("Install with: pip install google-cloud-storage")
    sys.exit(1)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def log_progress(message, level="info"):
    """Log progress with emojis and structured output"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    if level == "error":
        logger.error(f"{timestamp} - {message}")
    elif level == "warning":
        logger.warning(f"{timestamp} - {message}")
    elif level == "debug":
        logger.debug(f"{timestamp} - {message}")
    else:
        logger.info(f"{timestamp} - {message}")

class GCSExporter:
    """Handles export of files to Google Cloud Storage"""
    
    def __init__(self, project_id: str, bucket_name: str, credentials_path: Optional[str] = None):
        """
        Initialize GCS exporter
        
        Args:
            project_id: Google Cloud Project ID
            bucket_name: GCS bucket name
            credentials_path: Path to service account credentials JSON file
        """
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.credentials_path = credentials_path
        
        # Initialize GCS client with multiple authentication methods
        try:
            if credentials_path and os.path.exists(credentials_path):
                # Method 1: Service Account JSON file
                self.client = storage.Client.from_service_account_json(
                    credentials_path, project=project_id
                )
                log_progress(f"✅ Initialized GCS client with service account: {credentials_path}")
            else:
                # Method 2: Application Default Credentials or User Credentials
                # This will use:
                # - GOOGLE_APPLICATION_CREDENTIALS environment variable
                # - User credentials from 'gcloud auth application-default login'
                # - Compute Engine service account (if running on GCE)
                # - Cloud Shell credentials (if running in Cloud Shell)
                try:
                    self.client = storage.Client(project=project_id)
                    log_progress(f"✅ Initialized GCS client with application default credentials")
                    log_progress("ℹ️ Using one of: environment credentials, user login, or compute engine service account")
                except Exception as default_error:
                    log_progress(f"⚠️ Application default credentials failed: {default_error}", "warning")
                    # Method 3: Try without specifying project (let it auto-detect)
                    try:
                        self.client = storage.Client()
                        log_progress(f"✅ Initialized GCS client with auto-detected credentials")
                        log_progress("ℹ️ Project will be auto-detected from credentials")
                    except Exception as auto_error:
                        log_progress(f"❌ All authentication methods failed:", "error")
                        log_progress(f"   - Service account: {credentials_path or 'Not provided'}", "error")
                        log_progress(f"   - Default credentials: {default_error}", "error")
                        log_progress(f"   - Auto-detection: {auto_error}", "error")
                        raise Exception("No valid GCP authentication method found")
                
        except Exception as e:
            log_progress(f"❌ Failed to initialize GCS client: {e}", "error")
            raise
    
    def create_bucket_if_not_exists(self, location: str = "EUROPE-WEST1") -> bool:
        """
        Create GCS bucket if it doesn't exist
        
        Args:
            location: GCS bucket location
            
        Returns:
            True if bucket was created or already exists, False otherwise
        """
        try:
            # Check if bucket exists
            bucket = self.client.bucket(self.bucket_name)
            bucket.reload()
            log_progress(f"✅ Bucket '{self.bucket_name}' already exists")
            return True
            
        except NotFound:
            # Bucket doesn't exist, create it
            try:
                bucket = self.client.create_bucket(
                    self.bucket_name, 
                    location=location,
                    project=self.project_id
                )
                log_progress(f"✅ Created bucket '{self.bucket_name}' in location '{location}'")
                return True
                
            except Conflict:
                log_progress(f"⚠️ Bucket name '{self.bucket_name}' already exists globally", "warning")
                return False
            except Exception as e:
                log_progress(f"❌ Failed to create bucket '{self.bucket_name}': {e}", "error")
                return False
                
        except Exception as e:
            log_progress(f"❌ Error checking bucket '{self.bucket_name}': {e}", "error")
            return False
    
    def upload_file(self, local_path: str, gcs_path: str) -> bool:
        """
        Upload a single file to GCS
        
        Args:
            local_path: Local file path
            gcs_path: GCS destination path (relative to bucket)
            
        Returns:
            True if upload successful, False otherwise
        """
        try:
            if not os.path.exists(local_path):
                log_progress(f"⚠️ Local file not found: {local_path}", "warning")
                return False
            
            # Get file size for progress reporting
            file_size = os.path.getsize(local_path)
            file_size_mb = file_size / (1024 * 1024)
            
            bucket = self.client.bucket(self.bucket_name)
            blob = bucket.blob(gcs_path)
            
            log_progress(f"📤 Uploading {local_path} → gs://{self.bucket_name}/{gcs_path} ({file_size_mb:.2f} MB)")
            
            blob.upload_from_filename(local_path)
            
            log_progress(f"✅ Successfully uploaded: {gcs_path}")
            return True
            
        except Exception as e:
            log_progress(f"❌ Failed to upload {local_path}: {e}", "error")
            return False
    
    def upload_directory(self, local_dir: str, gcs_prefix: str = "") -> Dict[str, bool]:
        """
        Upload entire directory structure to GCS
        
        Args:
            local_dir: Local directory path
            gcs_prefix: GCS prefix (folder) to upload to
            
        Returns:
            Dictionary with file paths and upload status
        """
        results = {}
        
        if not os.path.exists(local_dir):
            log_progress(f"⚠️ Local directory not found: {local_dir}", "warning")
            return results
        
        log_progress(f"📁 Scanning directory: {local_dir}")
        
        # Walk through all files in directory
        for root, dirs, files in os.walk(local_dir):
            for file in files:
                local_file_path = os.path.join(root, file)
                
                # Calculate relative path from base directory
                rel_path = os.path.relpath(local_file_path, local_dir)
                
                # Create GCS path
                if gcs_prefix:
                    gcs_file_path = f"{gcs_prefix}/{rel_path}".replace("\\", "/")
                else:
                    gcs_file_path = rel_path.replace("\\", "/")
                
                # Upload file
                success = self.upload_file(local_file_path, gcs_file_path)
                results[local_file_path] = success
        
        return results

def get_gcs_config() -> Dict[str, str]:
    """Get GCS configuration from environment variables or config file"""
    
    # Try environment variables first
    project_id = os.getenv('GCS_PROJECT_ID')
    bucket_name = os.getenv('GCS_BUCKET_NAME')
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    
    # If not in environment, try to load from config file
    if not project_id or not bucket_name:
        config_file = '/opt/airflow/scripts/google_scraper/config/gcs_config.json'
        if os.path.exists(config_file):
            try:
                with open(config_file, 'r') as f:
                    config = json.load(f)
                    project_id = project_id or config.get('project_id')
                    bucket_name = bucket_name or config.get('bucket_name')
                    credentials_path = credentials_path or config.get('credentials_path')
                log_progress(f"✅ Loaded GCS config from: {config_file}")
            except Exception as e:
                log_progress(f"⚠️ Failed to load GCS config file: {e}", "warning")
    
    # Default values if not configured
    if not project_id:
        log_progress("⚠️ GCS_PROJECT_ID not configured, using default", "warning")
        project_id = "simprede-project"  # Replace with your project ID
    
    if not bucket_name:
        log_progress("⚠️ GCS_BUCKET_NAME not configured, using default", "warning")
        bucket_name = "simprede-data-pipeline"  # Replace with your bucket name
    
    return {
        'project_id': project_id,
        'bucket_name': bucket_name,
        'credentials_path': credentials_path
    }

def find_data_directories(base_data_dir: str, target_date: str) -> List[str]:
    """
    Find all data directories that should be exported for the target date
    
    Args:
        base_data_dir: Base data directory path
        target_date: Target date in YYYY-MM-DD format
        
    Returns:
        List of directory paths to export
    """
    directories = []
    
    # Parse date
    try:
        date_obj = datetime.strptime(target_date, '%Y-%m-%d')
        year = date_obj.strftime("%Y")
        month = date_obj.strftime("%m")
        day = date_obj.strftime("%d")
    except ValueError:
        log_progress(f"❌ Invalid date format: {target_date}", "error")
        return directories
    
    # Check each data type directory
    data_types = ['raw', 'structured', 'processed']
    
    for data_type in data_types:
        dir_path = os.path.join(base_data_dir, data_type, year, month, day)
        if os.path.exists(dir_path) and os.listdir(dir_path):
            directories.append(dir_path)
            log_progress(f"📁 Found data directory: {dir_path}")
        else:
            log_progress(f"⚠️ No data found in: {dir_path}", "warning")
    
    return directories

def find_airflow_logs(target_date: str, airflow_logs_dir: str = "/opt/airflow/logs") -> List[str]:
    """
    Find Airflow log directories for the target date
    
    Args:
        target_date: Target date in YYYY-MM-DD format
        airflow_logs_dir: Base Airflow logs directory
        
    Returns:
        List of log directory/file paths to export
    """
    log_paths = []
    
    # Parse date
    try:
        date_obj = datetime.strptime(target_date, '%Y-%m-%d')
        year = date_obj.strftime("%Y")
        month = date_obj.strftime("%m")
        day = date_obj.strftime("%d")
        date_str = date_obj.strftime("%Y-%m-%d")
    except ValueError:
        log_progress(f"❌ Invalid date format: {target_date}", "error")
        return log_paths
    
    # Common Airflow log patterns to look for
    log_patterns = [
        # DAG logs by date
        os.path.join(airflow_logs_dir, "dag_id=*", f"run_id=*{date_str}*"),
        os.path.join(airflow_logs_dir, "dag_id=*", f"execution_date={date_str}*"),
        
        # Scheduler logs
        os.path.join(airflow_logs_dir, "scheduler", f"{date_str}*"),
        os.path.join(airflow_logs_dir, "scheduler", year, month, day),
        
        # Task logs by date structure
        os.path.join(airflow_logs_dir, "*", "*", date_str),
        os.path.join(airflow_logs_dir, "*", "*", year, month, day),
        
        # Worker logs
        os.path.join(airflow_logs_dir, "worker*", f"*{date_str}*"),
        
        # Webserver logs
        os.path.join(airflow_logs_dir, "webserver*", f"*{date_str}*"),
    ]
    
    import glob
    
    for pattern in log_patterns:
        matches = glob.glob(pattern)
        for match in matches:
            if os.path.exists(match):
                if os.path.isfile(match):
                    log_paths.append(match)
                elif os.path.isdir(match) and os.listdir(match):
                    log_paths.append(match)
    
    # Also look for logs with today's date if they contain our target date
    if os.path.exists(airflow_logs_dir):
        for root, dirs, files in os.walk(airflow_logs_dir):
            for file in files:
                if (file.endswith('.log') or file.endswith('.txt')) and date_str in file:
                    full_path = os.path.join(root, file)
                    if full_path not in log_paths:
                        log_paths.append(full_path)
    
    # Remove duplicates and sort
    log_paths = list(set(log_paths))
    log_paths.sort()
    
    log_progress(f"📋 Found {len(log_paths)} Airflow log paths for {target_date}")
    for path in log_paths:
        log_progress(f"  📄 {path}")
    
    return log_paths

def export_airflow_logs_to_gcs(
    exporter: GCSExporter, 
    target_date: str, 
    airflow_logs_dir: str = "/opt/airflow/logs"
) -> Dict[str, bool]:
    """
    Export Airflow logs to GCS
    
    Args:
        exporter: GCSExporter instance
        target_date: Target date in YYYY-MM-DD format
        airflow_logs_dir: Base Airflow logs directory
        
    Returns:
        Dictionary with log paths and upload status
    """
    log_progress(f"📋 Starting Airflow logs export for {target_date}")
    
    # Find log paths
    log_paths = find_airflow_logs(target_date, airflow_logs_dir)
    
    if not log_paths:
        log_progress("ℹ️ No Airflow logs found for the target date", "warning")
        return {}
    
    results = {}
    
    for log_path in log_paths:
        try:
            if os.path.isfile(log_path):
                # Single file
                rel_path = os.path.relpath(log_path, airflow_logs_dir)
                gcs_path = f"airflow-logs/{target_date}/{rel_path}".replace("\\", "/")
                success = exporter.upload_file(log_path, gcs_path)
                results[log_path] = success
                
            elif os.path.isdir(log_path):
                # Directory - upload all files recursively
                rel_dir = os.path.relpath(log_path, airflow_logs_dir)
                gcs_prefix = f"airflow-logs/{target_date}/{rel_dir}".replace("\\", "/")
                dir_results = exporter.upload_directory(log_path, gcs_prefix)
                results.update(dir_results)
                
        except Exception as e:
            log_progress(f"❌ Failed to export log path {log_path}: {e}", "error")
            results[log_path] = False
    
    return results

def cleanup_data_directory(directory_path: str, dry_run: bool = False) -> bool:
    """
    Delete a data directory after successful upload
    
    Args:
        directory_path: Path to directory to delete
        dry_run: If True, only log what would be deleted without actually deleting
        
    Returns:
        True if deletion successful, False otherwise
    """
    try:
        if not os.path.exists(directory_path):
            log_progress(f"⚠️ Directory doesn't exist: {directory_path}", "warning")
            return True  # Consider it "successful" since it's already gone
        
        if dry_run:
            log_progress(f"🔍 DRY RUN: Would delete directory: {directory_path}")
            return True
        
        # Calculate directory size before deletion
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(directory_path):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                if os.path.exists(filepath):
                    total_size += os.path.getsize(filepath)
        
        size_mb = total_size / (1024 * 1024)
        
        log_progress(f"🗑️ Deleting directory: {directory_path} ({size_mb:.2f} MB)")
        shutil.rmtree(directory_path)
        log_progress(f"✅ Successfully deleted directory: {directory_path}")
        return True
        
    except Exception as e:
        log_progress(f"❌ Failed to delete directory {directory_path}: {e}", "error")
        return False

def cleanup_exported_data(
    directories_exported: List[str], 
    all_results: Dict[str, bool],
    cleanup_on_partial_success: bool = False,
    dry_run: bool = False
) -> Dict[str, bool]:
    """
    Clean up successfully exported data directories
    
    Args:
        directories_exported: List of directories that were exported
        all_results: Dictionary with file paths and upload status
        cleanup_on_partial_success: Whether to cleanup even if some files failed
        dry_run: If True, only log what would be deleted without actually deleting
        
    Returns:
        Dictionary with directory paths and cleanup status
    """
    cleanup_results = {}
    
    if not directories_exported:
        log_progress("ℹ️ No data directories to cleanup")
        return cleanup_results
    
    log_progress(f"🧹 Starting cleanup of {len(directories_exported)} data directories")
    
    for directory in directories_exported:
        try:
            # Check if all files in this directory were uploaded successfully
            directory_files = []
            failed_files_in_dir = []
            
            # Find all files that belong to this directory
            for file_path, success in all_results.items():
                if file_path.startswith(directory):
                    directory_files.append(file_path)
                    if not success:
                        failed_files_in_dir.append(file_path)
            
            # Decide whether to cleanup this directory
            should_cleanup = False
            
            if len(failed_files_in_dir) == 0:
                # All files uploaded successfully
                should_cleanup = True
                log_progress(f"✅ All files from {directory} uploaded successfully")
            elif cleanup_on_partial_success:
                # Some files failed but we're configured to cleanup anyway
                should_cleanup = True
                log_progress(f"⚠️ {len(failed_files_in_dir)} files failed from {directory}, but cleanup_on_partial_success=True")
            else:
                # Some files failed and we're not configured to cleanup
                should_cleanup = False
                log_progress(f"⚠️ Skipping cleanup of {directory} - {len(failed_files_in_dir)} files failed upload")
            
            if should_cleanup:
                success = cleanup_data_directory(directory, dry_run)
                cleanup_results[directory] = success
            else:
                cleanup_results[directory] = False
                
        except Exception as e:
            log_progress(f"❌ Error during cleanup evaluation for {directory}: {e}", "error")
            cleanup_results[directory] = False
    
    successful_cleanups = sum(1 for success in cleanup_results.values() if success)
    log_progress(f"🧹 Cleanup completed: {successful_cleanups}/{len(directories_exported)} directories cleaned")
    
    return cleanup_results

def export_to_gcs(
    target_date: str = None,
    base_data_dir: str = None,
    output_dir: str = None,
    gcs_project_id: str = None,
    gcs_bucket_name: str = None,
    gcs_credentials_path: str = None,
    include_airflow_logs: bool = True,
    airflow_logs_dir: str = "/opt/airflow/logs",
    cleanup_after_export: bool = True,
    cleanup_on_partial_success: bool = False,
    dry_run_cleanup: bool = False
) -> Dict[str, any]:
    """
    Main export function to upload files to GCS
    
    Args:
        target_date: Target date in YYYY-MM-DD format
        base_data_dir: Base directory containing data files
        output_dir: Directory to save export logs and statistics
        gcs_project_id: GCS project ID (optional, uses config if not provided)
        gcs_bucket_name: GCS bucket name (optional, uses config if not provided)
        gcs_credentials_path: Path to GCS credentials file (optional)
        include_airflow_logs: Whether to include Airflow logs in export
        airflow_logs_dir: Base Airflow logs directory
        cleanup_after_export: Whether to delete data directories after successful export
        cleanup_on_partial_success: Whether to cleanup even if some files failed
        dry_run_cleanup: If True, only log what would be deleted without actually deleting
        
    Returns:
        Dictionary with export statistics
    """
    
    if not target_date:
        target_date = datetime.now().strftime('%Y-%m-%d')
    
    if not base_data_dir:
        base_data_dir = "/opt/airflow/scripts/google_scraper/data"
    
    log_progress(f"🚀 Starting GCS export for date: {target_date}")
    log_progress(f"📁 Base data directory: {base_data_dir}")
    log_progress(f"📋 Include Airflow logs: {include_airflow_logs}")
    log_progress(f"🧹 Cleanup after export: {cleanup_after_export}")
    if cleanup_after_export:
        log_progress(f"🧹 Cleanup on partial success: {cleanup_on_partial_success}")
        log_progress(f"🧹 Dry run cleanup: {dry_run_cleanup}")
    
    # Get GCS configuration
    gcs_config = get_gcs_config()
    
    # Override with provided parameters
    if gcs_project_id:
        gcs_config['project_id'] = gcs_project_id
    if gcs_bucket_name:
        gcs_config['bucket_name'] = gcs_bucket_name
    if gcs_credentials_path:
        gcs_config['credentials_path'] = gcs_credentials_path
    
    log_progress(f"🔧 GCS Configuration:")
    log_progress(f"  - Project ID: {gcs_config['project_id']}")
    log_progress(f"  - Bucket: {gcs_config['bucket_name']}")
    log_progress(f"  - Credentials: {gcs_config['credentials_path'] or 'Default credentials'}")
    
    # Initialize GCS exporter
    try:
        exporter = GCSExporter(
            project_id=gcs_config['project_id'],
            bucket_name=gcs_config['bucket_name'],
            credentials_path=gcs_config['credentials_path']
        )
    except Exception as e:
        log_progress(f"❌ Failed to initialize GCS exporter: {e}", "error")
        raise
    
    # Create bucket if it doesn't exist
    if not exporter.create_bucket_if_not_exists():
        log_progress(f"❌ Failed to create or access bucket: {gcs_config['bucket_name']}", "error")
        raise Exception(f"Cannot access GCS bucket: {gcs_config['bucket_name']}")
    
    # Find directories to export
    directories_to_export = find_data_directories(base_data_dir, target_date)
    
    # Initialize statistics
    all_results = {}
    total_files = 0
    successful_uploads = 0
    failed_uploads = 0
    total_size_bytes = 0
    airflow_logs_uploaded = 0
    airflow_logs_failed = 0
    
    # Export data directories
    if directories_to_export:
        for local_dir in directories_to_export:
            log_progress(f"📤 Exporting data directory: {local_dir}")
            
            # Calculate GCS prefix based on relative path from base_data_dir
            rel_path = os.path.relpath(local_dir, base_data_dir)
            gcs_prefix = f"data/{rel_path}".replace("\\", "/")
            
            # Upload directory
            results = exporter.upload_directory(local_dir, gcs_prefix)
            all_results.update(results)
            
            # Count results
            for file_path, success in results.items():
                total_files += 1
                if success:
                    successful_uploads += 1
                    if os.path.exists(file_path):
                        total_size_bytes += os.path.getsize(file_path)
                else:
                    failed_uploads += 1
    else:
        log_progress("ℹ️ No data directories found for export", "warning")
    
    # Export Airflow logs
    if include_airflow_logs:
        log_progress("📋 Starting Airflow logs export...")
        try:
            airflow_results = export_airflow_logs_to_gcs(exporter, target_date, airflow_logs_dir)
            all_results.update(airflow_results)
            
            # Count Airflow log results
            for file_path, success in airflow_results.items():
                total_files += 1
                if success:
                    successful_uploads += 1
                    airflow_logs_uploaded += 1
                    if os.path.exists(file_path):
                        total_size_bytes += os.path.getsize(file_path)
                else:
                    failed_uploads += 1
                    airflow_logs_failed += 1
            
            log_progress(f"📋 Airflow logs export completed: {airflow_logs_uploaded} uploaded, {airflow_logs_failed} failed")
            
        except Exception as e:
            log_progress(f"❌ Failed to export Airflow logs: {e}", "error")
    
    total_size_mb = total_size_bytes / (1024 * 1024)
    
    # Cleanup data directories if requested
    cleanup_results = {}
    if cleanup_after_export and directories_to_export:
        log_progress("🧹 Starting cleanup of exported data directories...")
        cleanup_results = cleanup_exported_data(
            directories_to_export, 
            all_results, 
            cleanup_on_partial_success,
            dry_run_cleanup
        )
    
    export_stats = {
        "target_date": target_date,
        "bucket_name": gcs_config['bucket_name'],
        "data_directories_found": len(directories_to_export) if directories_to_export else 0,
        "data_directories_exported": directories_to_export or [],
        "files_uploaded": successful_uploads,
        "files_failed": failed_uploads,
        "total_files": total_files,
        "airflow_logs_uploaded": airflow_logs_uploaded,
        "airflow_logs_failed": airflow_logs_failed,
        "total_size_mb": round(total_size_mb, 2),
        "upload_results": all_results,
        "cleanup_enabled": cleanup_after_export,
        "cleanup_results": cleanup_results,
        "directories_cleaned": sum(1 for success in cleanup_results.values() if success) if cleanup_results else 0,
        "included_airflow_logs": include_airflow_logs,
        "status": "success" if failed_uploads == 0 else "partial_success"
    }
    
    log_progress(f"✅ Export completed:")
    log_progress(f"  - Data directories: {len(directories_to_export) if directories_to_export else 0}")
    log_progress(f"  - Data files uploaded: {successful_uploads - airflow_logs_uploaded}/{total_files - airflow_logs_uploaded - airflow_logs_failed}")
    log_progress(f"  - Airflow logs uploaded: {airflow_logs_uploaded}")
    log_progress(f"  - Total files uploaded: {successful_uploads}/{total_files}")
    log_progress(f"  - Total size: {total_size_mb:.2f} MB")
    log_progress(f"  - Bucket: gs://{gcs_config['bucket_name']}")
    
    if cleanup_after_export:
        directories_cleaned = sum(1 for success in cleanup_results.values() if success) if cleanup_results else 0
        log_progress(f"  - Directories cleaned: {directories_cleaned}/{len(directories_to_export) if directories_to_export else 0}")
    
    # Save export statistics
    if output_dir:
        try:
            os.makedirs(output_dir, exist_ok=True)
            
            date_compact = target_date.replace('-', '')
            stats_file = os.path.join(output_dir, f"gcs_export_stats_{date_compact}.json")
            
            # Add timestamp and additional metadata
            enhanced_stats = {
                "export_timestamp": datetime.now().isoformat(),
                "export_date": target_date,
                **export_stats
            }
            
            with open(stats_file, 'w', encoding='utf-8') as f:
                json.dump(enhanced_stats, f, indent=2, ensure_ascii=False)
            
            log_progress(f"✅ Export statistics saved: {stats_file}")
            
        except Exception as e:
            log_progress(f"⚠️ Failed to save export statistics: {e}", "warning")
    
    return export_stats

def main():
    """Main function with command line argument support"""
    parser = argparse.ArgumentParser(description="Export SIMPREDE pipeline files to Google Cloud Storage")
    parser.add_argument("--date", type=str, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--base_data_dir", type=str, help="Base data directory path")
    parser.add_argument("--output_dir", type=str, help="Output directory for logs and statistics")
    parser.add_argument("--gcs_project_id", type=str, help="GCS Project ID")
    parser.add_argument("--gcs_bucket_name", type=str, help="GCS Bucket name")
    parser.add_argument("--gcs_credentials_path", type=str, help="Path to GCS credentials JSON file")
    parser.add_argument("--no_airflow_logs", action="store_true", help="Skip Airflow logs export")
    parser.add_argument("--airflow_logs_dir", type=str, default="/opt/airflow/logs", help="Airflow logs directory")
    parser.add_argument("--no_cleanup", action="store_true", help="Skip cleanup of data directories after export")
    parser.add_argument("--cleanup_on_partial_success", action="store_true", help="Cleanup directories even if some files failed")
    parser.add_argument("--dry_run_cleanup", action="store_true", help="Only log what would be deleted without actually deleting")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    if args.debug:
        logger.setLevel(logging.DEBUG)
        log_progress("🔍 DEBUG LOGGING enabled", "debug")
    
    log_progress("Starting export_to_gcs_airflow")
    log_progress(f"Parameters: date={args.date}")
    log_progress(f"Paths: base_data_dir={args.base_data_dir}, output_dir={args.output_dir}")
    log_progress(f"GCS: project_id={args.gcs_project_id}, bucket={args.gcs_bucket_name}")
    log_progress(f"Airflow logs: include={not args.no_airflow_logs}, dir={args.airflow_logs_dir}")
    log_progress(f"Cleanup: enabled={not args.no_cleanup}, partial_success={args.cleanup_on_partial_success}, dry_run={args.dry_run_cleanup}")
    
    try:
        result = export_to_gcs(
            target_date=args.date,
            base_data_dir=args.base_data_dir,
            output_dir=args.output_dir,
            gcs_project_id=args.gcs_project_id,
            gcs_bucket_name=args.gcs_bucket_name,
            gcs_credentials_path=args.gcs_credentials_path,
            include_airflow_logs=not args.no_airflow_logs,
            airflow_logs_dir=args.airflow_logs_dir,
            cleanup_after_export=not args.no_cleanup,
            cleanup_on_partial_success=args.cleanup_on_partial_success,
            dry_run_cleanup=args.dry_run_cleanup
        )
        
        log_progress(f"✅ GCS export completed successfully")
        log_progress(f"📊 Summary: {result['files_uploaded']} files uploaded ({result['airflow_logs_uploaded']} logs), {result['total_size_mb']} MB")
        if result.get('cleanup_enabled'):
            log_progress(f"🧹 Cleanup: {result['directories_cleaned']} directories cleaned")
        return 0
        
    except Exception as e:
        log_progress(f"❌ GCS export failed: {e}", "error")
        log_progress(f"❌ Full traceback: {traceback.format_exc()}", "error")
        return 1

