#!/usr/bin/env python3
"""
Verify Chrome Installation in Docker Container
This script checks and validates that Chrome and ChromeDriver are properly installed
and accessible within the Docker container.
"""
import os
import sys
import subprocess
import platform
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("verify_chrome")

def run_command(command):
    """Run a shell command and return output and error code"""
    try:
        output = subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT).decode().strip()
        return output, 0
    except subprocess.CalledProcessError as e:
        return e.output.decode().strip() if e.output else f"Command failed with code {e.returncode}", e.returncode

def main():
    """Main verification function"""
    logger.info("Starting Chrome/ChromeDriver verification")
    
    # System info
    logger.info(f"Python version: {platform.python_version()}")
    logger.info(f"Platform: {platform.platform()}")
    
    # Check environment variables
    chrome_bin = os.environ.get('CHROME_BIN')
    chromedriver_path = os.environ.get('CHROMEDRIVER_PATH')
    
    logger.info(f"CHROME_BIN environment variable: {chrome_bin}")
    logger.info(f"CHROMEDRIVER_PATH environment variable: {chromedriver_path}")
    
    # Check if CHROME_BIN exists and is executable
    if chrome_bin:
        exists = os.path.exists(chrome_bin)
        is_executable = os.access(chrome_bin, os.X_OK) if exists else False
        logger.info(f"CHROME_BIN exists: {exists}, executable: {is_executable}")
    
    # Check if CHROMEDRIVER_PATH exists and is executable
    if chromedriver_path:
        exists = os.path.exists(chromedriver_path)
        is_executable = os.access(chromedriver_path, os.X_OK) if exists else False
        logger.info(f"CHROMEDRIVER_PATH exists: {exists}, executable: {is_executable}")
    
    # Try to get Chrome version
    logger.info("Checking Chrome version:")
    output, code = run_command("google-chrome --version")
    if code == 0:
        logger.info(f"Chrome version: {output}")
    else:
        logger.error(f"Failed to get Chrome version: {output}")
    
    # Try to get ChromeDriver version
    logger.info("Checking ChromeDriver version:")
    output, code = run_command("chromedriver --version")
    if code == 0:
        logger.info(f"ChromeDriver version: {output}")
    else:
        logger.error(f"Failed to get ChromeDriver version: {output}")
    
    # Create and verify symbolic links
    logger.info("Checking symbolic links:")
    for link in ["/usr/bin/chromium", "/usr/bin/chromium-browser"]:
        if os.path.exists(link):
            target = os.path.realpath(link)
            logger.info(f"{link} -> {target}")
        else:
            logger.error(f"{link} does not exist")
    
    # Create a simple Selenium test
    logger.info("Testing Selenium with Chrome:")
    test_script = """
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

options = Options()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')

service = Service('/usr/bin/chromedriver')
driver = webdriver.Chrome(service=service, options=options)
driver.get('https://example.com')
print(f"Page title: {driver.title}")
driver.quit()
print("Selenium test completed successfully")
"""
    
    # Write the test script to a temporary file
    with open("/tmp/selenium_test.py", "w") as f:
        f.write(test_script)
    
    # Run the test script
    logger.info("Running Selenium test script:")
    output, code = run_command("python3 /tmp/selenium_test.py")
    if code == 0:
        logger.info(f"Selenium test result: {output}")
        logger.info("✅ Selenium test PASSED")
    else:
        logger.error(f"Selenium test failed: {output}")
        logger.error("❌ Selenium test FAILED")
    
    # Remove the temporary file
    os.remove("/tmp/selenium_test.py")
    
    # Final summary
    logger.info("\nVerification Summary:")
    logger.info(f"Chrome binary: {'✅ OK' if chrome_bin and os.path.exists(chrome_bin) else '❌ MISSING'}")
    logger.info(f"ChromeDriver: {'✅ OK' if chromedriver_path and os.path.exists(chromedriver_path) else '❌ MISSING'}")
    logger.info(f"Chrome version check: {'✅ PASSED' if code == 0 else '❌ FAILED'}")
    logger.info(f"Selenium test: {'✅ PASSED' if code == 0 else '❌ FAILED'}")
    
    return 0 if chrome_bin and chromedriver_path and code == 0 else 1

if __name__ == "__main__":
    sys.exit(main())
