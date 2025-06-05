import undetected_chromedriver as uc
from selenium.webdriver.chrome.options import Options
import psutil
import atexit
import time

class BrowserManager:
    def __init__(self):
        self.driver = None
        atexit.register(self.cleanup)
        
    def get_driver(self):
        if self.driver is None:
            options = Options()
            options.add_argument('--headless')
            options.add_argument('--disable-gpu')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument("--remote-debugging-port=9222")
            self.driver = uc.Chrome(options=options)
        return self.driver
    
    def cleanup(self):
        try:
            if self.driver:
                self.driver.quit()
                time.sleep(1)  # Give Chrome time to close properly
            
            # Kill any remaining Chrome processes
            for proc in psutil.process_iter(['pid', 'name']):
                try:
                    if 'chrome' in proc.info['name'].lower():
                        proc.kill()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
        except Exception as e:
            print(f"Error during cleanup: {e}")