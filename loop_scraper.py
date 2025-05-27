import time
import subprocess

while True:
    print("🔁 Starting scrape...")
    subprocess.run(["python", "padelv2.py"])
    print("✅ Scrape complete. Sleeping for 24 hours...\n")
    time.sleep(6 * 60 * 60)  # 24 hours
