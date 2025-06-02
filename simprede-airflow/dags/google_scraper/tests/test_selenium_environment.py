#!/usr/bin/env python3
"""
Test script to verify that Selenium and Chrome are properly configured in the Airflow container.
This script can be run via the Airflow task to validate the environment.
"""
import os
import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("test_selenium_environment")

def test_selenium_installation():
    """Test that Selenium is properly installed and configured"""
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.chrome.options import Options
        from webdriver_manager.chrome import ChromeDriverManager
        
        logger.info("✅ Selenium and WebDriver Manager imports successful")
        return True
    except ImportError as e:
        logger.error(f"❌ Selenium import error: {e}")
        return False

def test_chrome_environment():
    """Test that Chrome/Chromium is properly installed and configured"""
    try:
        # Check for Chrome binary in various possible locations
        possible_chrome_paths = [
            os.environ.get('CHROME_BIN'),  # Check environment variable first
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
        possible_chrome_paths = [path for path in possible_chrome_paths if path]
        
        # Check if any of the paths exist
        chrome_found = False
        for path in possible_chrome_paths:
            if os.path.exists(path):
                logger.info(f"✅ Chrome/Chromium found at: {path}")
                chrome_path = path
                chrome_found = True
                break
        
        if not chrome_found:
            logger.error(f"❌ Chrome/Chromium not found in any of these locations: {', '.join(possible_chrome_paths)}")
            return False
        
        # Check for ChromeDriver in various possible locations
        possible_driver_paths = [
            os.environ.get('CHROMEDRIVER_PATH'),  # Check environment variable first
            '/usr/bin/chromedriver',
            '/usr/local/bin/chromedriver',
            '/snap/bin/chromedriver',
        ]
        
        # Filter out None values
        possible_driver_paths = [path for path in possible_driver_paths if path]
        
        # Check if any of the paths exist
        driver_found = False
        for path in possible_driver_paths:
            if os.path.exists(path):
                logger.info(f"✅ ChromeDriver found at: {path}")
                driver_found = True
                break
        
        if not driver_found:
            logger.error(f"❌ ChromeDriver not found in any of these locations: {', '.join(possible_driver_paths)}")
            return False
        
        return True
    except Exception as e:
        logger.error(f"❌ Error checking Chrome environment: {e}")
        return False

def test_headless_browser():
    """Test that a headless browser can be launched"""
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        
        # Try to use webdriver_manager if available
        try:
            from webdriver_manager.chrome import ChromeDriverManager
            use_webdriver_manager = True
            logger.info("Using webdriver_manager for ChromeDriver")
        except ImportError:
            use_webdriver_manager = False
            logger.warning("webdriver_manager not available, using system ChromeDriver")
        
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        
        # Find Chrome binary
        chrome_binary = None
        possible_chrome_paths = [
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
        
        # Filter out None values and find first existing path
        for path in [p for p in possible_chrome_paths if p]:
            if os.path.exists(path):
                chrome_binary = path
                logger.info(f"Using Chrome binary at: {chrome_binary}")
                options.binary_location = chrome_binary
                break
        
        if not chrome_binary:
            logger.error("No Chrome binary found, cannot run headless browser test")
            return False
        
        logger.info("Attempting to start headless Chrome...")
        
        # Create driver using appropriate method
        if use_webdriver_manager:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
        else:
            driver = webdriver.Chrome(options=options)
        
        # Simple test
        driver.get("https://www.google.com")
        logger.info(f"✅ Browser title: {driver.title}")
        
        driver.quit()
        logger.info("✅ Headless browser test successful")
        return True
    except Exception as e:
        logger.error(f"❌ Error running headless browser: {e}")
        return False

def main():
    """Run all tests"""
    logger.info("Starting Selenium environment tests...")
    
    selenium_ok = test_selenium_installation()
    chrome_env_ok = test_chrome_environment()
    
    if selenium_ok and chrome_env_ok:
        browser_test_ok = test_headless_browser()
    else:
        logger.warning("Skipping browser test due to failed prerequisite checks")
        browser_test_ok = False
    
    # Summary
    logger.info("\nTest Summary:")
    logger.info(f"Selenium Installation: {'✅ PASS' if selenium_ok else '❌ FAIL'}")
    logger.info(f"Chrome Environment: {'✅ PASS' if chrome_env_ok else '❌ FAIL'}")
    logger.info(f"Headless Browser: {'✅ PASS' if browser_test_ok else '❌ FAIL'}")
    
    # Return code based on test results
    if selenium_ok and chrome_env_ok and browser_test_ok:
        logger.info("✅ All tests passed")
        return 0
    else:
        logger.error("❌ Some tests failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())
