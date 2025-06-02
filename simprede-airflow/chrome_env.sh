#!/bin/bash
# This script ensures Chrome is in the PATH

# Add /usr/bin to PATH if not already there
if [[ ":$PATH:" != *":/usr/bin:"* ]]; then
    export PATH="/usr/bin:${PATH}"
fi

# Detect the architecture
ARCH=$(uname -m)

# Set Chrome binary based on what's available
if [ -x "/usr/bin/chromium" ]; then
    export CHROME_BIN=/usr/bin/chromium
elif [ -x "/usr/bin/google-chrome" ]; then
    export CHROME_BIN=/usr/bin/google-chrome
else
    echo "WARNING: Neither Chromium nor Google Chrome found!"
    # On ARM, try to symlink chromium to google-chrome if it exists
    if [ "$ARCH" != "x86_64" ] && [ -x "/usr/bin/chromium" ]; then
        ln -sf /usr/bin/chromium /usr/bin/google-chrome
        export CHROME_BIN=/usr/bin/chromium
    fi
fi

# Set ChromeDriver path
if [ -x "/usr/bin/chromedriver" ]; then
    export CHROMEDRIVER_PATH=/usr/bin/chromedriver
else
    echo "WARNING: ChromeDriver not found at /usr/bin/chromedriver"
fi

# Set display for headless operation
export DISPLAY=:99
export SELENIUM_HEADLESS=1

# Print environment information for debugging
echo "Chrome environment setup (arch: $ARCH):"
echo "PATH: $PATH"
echo "CHROME_BIN: $CHROME_BIN"
echo "CHROMEDRIVER_PATH: $CHROMEDRIVER_PATH"
echo "DISPLAY: $DISPLAY"
echo "SELENIUM_HEADLESS: $SELENIUM_HEADLESS"

# Check Chrome and ChromeDriver
if [ -n "$CHROME_BIN" ] && [ -x "$CHROME_BIN" ]; then
    echo "Chrome found and executable at $CHROME_BIN"
    $CHROME_BIN --version || echo "Failed to get Chrome version"
else
    echo "WARNING: Chrome not found or not executable at $CHROME_BIN"
fi

if [ -n "$CHROMEDRIVER_PATH" ] && [ -x "$CHROMEDRIVER_PATH" ]; then
    echo "ChromeDriver found and executable at $CHROMEDRIVER_PATH"
    $CHROMEDRIVER_PATH --version || echo "Failed to get ChromeDriver version"
else
    echo "WARNING: ChromeDriver not found or not executable at $CHROMEDRIVER_PATH"
fi
