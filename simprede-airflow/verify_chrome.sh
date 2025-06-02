#!/bin/bash
# Script to verify Chrome installation and debug issues

# Print system information
echo "===== System Information ====="
echo "System architecture: $(uname -m)"
echo "OS: $(cat /etc/os-release | grep PRETTY_NAME | cut -d= -f2 | tr -d '"')"
echo "Kernel: $(uname -r)"
echo "============================"

# Check for Chrome/Chromium binary
echo -e "\n===== Chrome/Chromium Check ====="
for path in /usr/bin/google-chrome /usr/bin/chromium /usr/bin/chromium-browser; do
    if [ -f "$path" ]; then
        echo "✅ Found at: $path"
        if [ -x "$path" ]; then
            echo "  - Executable: Yes"
        else
            echo "  - Executable: No"
            echo "  - Fixing permissions..."
            chmod +x "$path"
        fi
    else
        echo "❌ Not found at: $path"
    fi
done

# Check if ChromeDriver is installed
echo -e "\nChecking for ChromeDriver binary..."
if [ -f "/usr/bin/chromedriver" ]; then
    echo "✅ Found at: /usr/bin/chromedriver"
    if [ -x "/usr/bin/chromedriver" ]; then
        echo "  - Executable: Yes"
    else
        echo "  - Executable: No"
        echo "  - Fixing permissions..."
        chmod +x "/usr/bin/chromedriver"
    fi
else
    echo "❌ Not found at: /usr/bin/chromedriver"
fi

# Check environment variables
echo -e "\nChecking environment variables..."
if [ -n "$CHROME_BIN" ]; then
    echo "✅ CHROME_BIN=$CHROME_BIN"
else
    echo "❌ CHROME_BIN not set"
    # Try to find Chrome/Chromium and set the variable
    for chrome_path in /usr/bin/chromium /usr/bin/google-chrome; do
        if [ -x "$chrome_path" ]; then
            echo "  - Setting CHROME_BIN=$chrome_path"
            export CHROME_BIN=$chrome_path
            break
        fi
    done
fi

if [ -n "$CHROMEDRIVER_PATH" ]; then
    echo "✅ CHROMEDRIVER_PATH=$CHROMEDRIVER_PATH"
else
    echo "❌ CHROMEDRIVER_PATH not set"
    if [ -x "/usr/bin/chromedriver" ]; then
        echo "  - Setting CHROMEDRIVER_PATH=/usr/bin/chromedriver"
        export CHROMEDRIVER_PATH=/usr/bin/chromedriver
    fi
fi

# Try running Chrome/Chromium
echo -e "\nTrying to run Chrome/Chromium..."
for chrome_cmd in "$CHROME_BIN" /usr/bin/chromium /usr/bin/google-chrome; do
    if [ -x "$chrome_cmd" ]; then
        echo "Trying $chrome_cmd --version:"
        $chrome_cmd --version || echo "Failed to get version"
        break
    fi
done

# Try running ChromeDriver
echo -e "\nTrying to run ChromeDriver..."
if [ -x "/usr/bin/chromedriver" ]; then
    /usr/bin/chromedriver --version || echo "Failed to get ChromeDriver version"
else
    echo "Cannot run ChromeDriver - not executable"
fi

# Create symlinks if needed
echo -e "\nEnsuring symlinks are properly set up..."
if [ -x "/usr/bin/chromium" ] && [ ! -x "/usr/bin/google-chrome" ]; then
    echo "Creating symlink: /usr/bin/google-chrome -> /usr/bin/chromium"
    ln -sf /usr/bin/chromium /usr/bin/google-chrome
fi

if [ -x "/usr/bin/google-chrome" ] && [ ! -x "/usr/bin/chromium" ]; then
    echo "Creating symlink: /usr/bin/chromium -> /usr/bin/google-chrome"
    ln -sf /usr/bin/google-chrome /usr/bin/chromium
fi

# Print PATH
echo -e "\nCurrent PATH:"
echo $PATH

# Print PATH for airflow user
echo -e "\nPATH for airflow user:"
if command -v gosu &> /dev/null; then
    gosu airflow bash -c 'echo $PATH'
else
    echo "gosu not available, cannot check airflow user PATH"
fi

echo -e "\nVerification complete."
