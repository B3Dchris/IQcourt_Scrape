#!/usr/bin/env bash
set -e  # Exit on error
set -o pipefail  # Catch pipe failures
set -x  # Debug print all commands

# Update and install dependencies
apt-get update
apt-get install -y wget gnupg unzip curl ca-certificates

# Install Google Chrome
CHROME_DEB="google-chrome-stable_current_amd64.deb"
wget https://dl.google.com/linux/direct/$CHROME_DEB
apt install -y ./$CHROME_DEB

# Determine installed Chrome major version
CHROME_VERSION=$(google-chrome --version | awk '{ print $3 }' | cut -d '.' -f 1)
echo "Installed Chrome major version: $CHROME_VERSION"

# Install matching ChromeDriver
CHROMEDRIVER_URL="https://chromedriver.storage.googleapis.com/${CHROME_VERSION}.0.0/chromedriver_linux64.zip"
wget -q $CHROMEDRIVER_URL -O chromedriver.zip || {
    echo "❌ Failed to download ChromeDriver for version $CHROME_VERSION"
    exit 1
}
unzip chromedriver.zip
mv chromedriver /usr/local/bin/chromedriver
chmod +x /usr/local/bin/chromedriver

# Confirm versions
google-chrome --version
chromedriver --version

# Install Python deps
pip install --upgrade pip
pip install -r requirements.txt
