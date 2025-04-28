#!/bin/bash

# Install Chrome
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
apt install -y ./google-chrome-stable_current_amd64.deb

# Install ChromeDriver matching Chrome version
CHROME_VERSION=$(google-chrome --version | awk '{ print $3 }' | cut -d '.' -f 1)
wget https://chromedriver.storage.googleapis.com/${CHROME_VERSION}.0.0/chromedriver_linux64.zip
unzip chromedriver_linux64.zip
mv chromedriver /usr/local/bin/chromedriver
chmod +x /usr/local/bin/chromedriver

# Install python deps
pip install -r requirements.txt
