#!/usr/bin/env python3
"""
Simple script to check Chrome installation in the container.
This will print information about the Chrome installation.
"""
import os
import sys
import subprocess
import platform

def check_chrome_installation():
    """Check Chrome installation and print detailed information"""
    print(f"Python version: {platform.python_version()}")
    print(f"Platform: {platform.platform()}")
    
    # Check environment variables
    chrome_bin = os.environ.get('CHROME_BIN')
    chromedriver_path = os.environ.get('CHROMEDRIVER_PATH')
    
    print(f"CHROME_BIN environment variable: {chrome_bin}")
    print(f"CHROMEDRIVER_PATH environment variable: {chromedriver_path}")
    
    # Check common Chrome binary locations
    chrome_locations = [
        '/usr/bin/chromium',
        '/usr/bin/chromium-browser',
        '/usr/bin/google-chrome',
        '/usr/bin/google-chrome-stable',
        '/usr/local/bin/chromium',
        '/usr/local/bin/chromium-browser',
        '/usr/local/bin/google-chrome',
        '/snap/bin/chromium',
    ]
    
    print("\nChecking Chrome binary locations:")
    for location in chrome_locations:
        exists = os.path.exists(location)
        is_executable = os.access(location, os.X_OK) if exists else False
        status = "✅ EXISTS and EXECUTABLE" if is_executable else ("❌ EXISTS but NOT EXECUTABLE" if exists else "❌ NOT FOUND")
        print(f"{location}: {status}")
    
    # Check common ChromeDriver locations
    driver_locations = [
        '/usr/bin/chromedriver',
        '/usr/local/bin/chromedriver',
        '/snap/bin/chromedriver',
    ]
    
    print("\nChecking ChromeDriver locations:")
    for location in driver_locations:
        exists = os.path.exists(location)
        is_executable = os.access(location, os.X_OK) if exists else False
        status = "✅ EXISTS and EXECUTABLE" if is_executable else ("❌ EXISTS but NOT EXECUTABLE" if exists else "❌ NOT FOUND")
        print(f"{location}: {status}")
    
    # Try to get Chrome version using different commands
    print("\nAttempting to get Chrome version:")
    commands = [
        "chromium --version",
        "chromium-browser --version",
        "google-chrome --version",
        "/usr/bin/chromium --version",
        "/usr/bin/chromium-browser --version",
        "/usr/bin/google-chrome --version"
    ]
    
    for cmd in commands:
        try:
            output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT).decode().strip()
            print(f"✅ {cmd}: {output}")
        except subprocess.CalledProcessError:
            print(f"❌ {cmd}: Command failed")
    
    # Check if required libraries for Chrome are installed
    print("\nChecking for required libraries:")
    libraries = [
        "libnspr4",
        "libnss3",
        "libexpat1",
        "libfontconfig1",
        "libgbm1",
        "libglib2.0-0"
    ]
    
    for lib in libraries:
        try:
            output = subprocess.check_output(f"ldconfig -p | grep {lib}", shell=True, stderr=subprocess.STDOUT).decode().strip()
            print(f"✅ {lib}: Found")
        except subprocess.CalledProcessError:
            print(f"❌ {lib}: Not found")

if __name__ == "__main__":
    try:
        check_chrome_installation()
    except Exception as e:
        print(f"Error during Chrome installation check: {e}")
        sys.exit(1)
