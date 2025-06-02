import os
import sys
import psutil
import atexit
import time
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

# For suppressing undetected_chromedriver import warnings if not used
import warnings
warnings.filterwarnings("ignore", category=ImportWarning)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("browser_manager")

class BrowserManager:
    def __init__(self):
        self.driver = None
        self.chrome_binary = None
        atexit.register(self.cleanup)
        
    def find_chrome_binary(self):
        """Find Chrome binary in various locations"""
        possible_paths = [
            os.environ.get('CHROME_BIN'),
            '/usr/bin/chromium',
            '/usr/bin/chromium-browser',
            '/usr/bin/google-chrome',
            '/usr/bin/google-chrome-stable',
            '/usr/local/bin/chromium',
            '/usr/local/bin/chromium-browser',
            '/usr/local/bin/google-chrome',
            '/snap/bin/chromium',
        ]
        
        # Filter out None values
        possible_paths = [p for p in possible_paths if p]
        
        for path in possible_paths:
            if os.path.exists(path) and os.access(path, os.X_OK):
                logger.info(f"Found Chrome binary at: {path}")
                return path
                
        # If no executable found, check if any file exists but isn't executable
        for path in possible_paths:
            if os.path.exists(path):
                logger.warning(f"Chrome binary found at {path} but not executable. Fixing permissions...")
                try:
                    os.chmod(path, 0o755)  # Make executable
                    logger.info(f"Made Chrome binary at {path} executable")
                    return path
                except Exception as e:
                    logger.error(f"Failed to make Chrome binary executable: {e}")
        
        logger.warning("Chrome binary not found in any of the standard locations!")
        return None
        
    def get_driver(self, use_undetected=False):
        """Get a Chrome webdriver instance.
        
        Args:
            use_undetected: If True, try to use undetected_chromedriver.
                           Falls back to regular Chrome if unavailable.
        """
        if self.driver is not None:
            return self.driver
            
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument("--remote-debugging-port=9222")
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-software-rasterizer')
        
        # User agent to avoid detection
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36')
        
        # Find Chrome binary location
        if not self.chrome_binary:
            self.chrome_binary = self.find_chrome_binary()
            if self.chrome_binary:
                options.binary_location = self.chrome_binary
                logger.info(f"Using Chrome binary at: {self.chrome_binary}")
            else:
                logger.warning("Chrome binary not found! This might cause issues.")
        
        # Check for chromedriver
        chromedriver_path = os.environ.get('CHROMEDRIVER_PATH') or '/usr/bin/chromedriver'
        if os.path.exists(chromedriver_path):
            logger.info(f"Using ChromeDriver at: {chromedriver_path}")
            service = Service(executable_path=chromedriver_path)
        else:
            logger.warning(f"ChromeDriver not found at {chromedriver_path}. Will use default location.")
            service = Service()
        
        try:
            if use_undetected:
                # Skip this section if running on ARM architecture
                if os.uname().machine.startswith('arm') or os.uname().machine.startswith('aarch64'):
                    logger.warning("Running on ARM architecture, skipping undetected_chromedriver")
                    self.driver = webdriver.Chrome(service=service, options=options)
                else:
                    try:
                        import undetected_chromedriver as uc
                        self.driver = uc.Chrome(options=options)
                        logger.info("Using undetected_chromedriver")
                    except (ImportError, Exception) as e:
                        logger.warning(f"Failed to use undetected_chromedriver: {e}")
                        logger.info("Falling back to standard Chrome webdriver")
                        self.driver = webdriver.Chrome(service=service, options=options)
            else:
                self.driver = webdriver.Chrome(service=service, options=options)
                
            logger.info("Chrome webdriver initialized successfully")
            return self.driver
            
        except Exception as e:
            logger.error(f"Error initializing Chrome webdriver: {e}")
            raise
    
    def cleanup(self):
        """Clean up webdriver and Chrome processes"""
        try:
            if self.driver:
                logger.info("Closing Chrome webdriver")
                self.driver.quit()
                time.sleep(1)  # Give Chrome time to close properly
                self.driver = None
            
            # Kill any remaining Chrome processes
            for proc in psutil.process_iter(['pid', 'name']):
                try:
                    if 'chrome' in proc.info['name'].lower():
                        logger.info(f"Killing Chrome process: {proc.info}")
                        proc.kill()
                except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
                    logger.warning(f"Could not kill Chrome process: {e}")
                except Exception as e:
                    logger.error(f"Error killing Chrome process: {e}")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")