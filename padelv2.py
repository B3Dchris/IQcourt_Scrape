#!/usr/bin/env python3
import os
import json
import time
import uuid
import math
import random
import logging
import requests
import re
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from webdriver_manager.chrome import ChromeDriverManager    

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from supabase import create_client
from dotenv import load_dotenv

# ——— Configuration —————————————————————————————————————————————
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

BASE_DIR = Path(__file__).parent
SCREENSHOTS_DIR = BASE_DIR / "webscrape" / "Scraped data" / "screenshots"
JSON_DIR        = BASE_DIR / "webscrape" / "Scraped data" / "supabase_ready"
SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)
JSON_DIR.mkdir(parents=True, exist_ok=True)

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

PROXY_USER = os.getenv("SMARTPROXY_USER")
PROXY_PASS = os.getenv("SMARTPROXY_PASS")
PROXY_HOST = os.getenv("SMARTPROXY_HOST")
PROXY_PORTS = os.getenv("SMARTPROXY_PORTS", "10001").split(",")
CHROME_VERSION = int(os.getenv("CHROME_VERSION_MAIN", "122"))

# —————————————————————————————————————————————————————————————————

def fetch_club_urls():
    try:
        resp = supabase.table("clubs").select("id,name,url").execute()
        return resp.data or []
    except Exception as e:
        logging.error(f"fetch_club_urls: {e}")
        return []

def create_scrape_run():
    payload = {
        "run_at": datetime.utcnow().isoformat(),
        "booking_date": datetime.utcnow().strftime("%Y-%m-%d"),
        "source": Path(__file__).name,
        "notes": "Automated scrape of playtomic courts",
        "slots_scraped": 0,
        "clubs_covered": 0,
        "scrape_status": "in_progress"
    }
    try:
        resp = supabase.table("scrape_runs").insert(payload).execute()
        return resp.data[0]["id"]
    except Exception as e:
        logging.error(f"create_scrape_run: {e}")
        return None

def update_scrape_run(scrape_id, status, slots, clubs):
    payload = {
        "scrape_status": status,
        "slots_scraped": slots,
        "clubs_covered": clubs
    }
    try:
        supabase.table("scrape_runs").update(payload).eq("id", scrape_id).execute()
    except Exception as e:
        logging.error(f"update_scrape_run: {e}")

def ensure_court_exists(club_id, name):
    if not club_id:
        return None
    try:
        resp = (
            supabase.table("courts")
            .select("id")
            .eq("club_id", club_id)
            .eq("name", name)
            .execute()
        )
        if resp.data:
            return resp.data[0]["id"]
        payload = {
            "club_id": club_id,
            "name": name,
            "created_at": datetime.utcnow().isoformat()
        }
        resp = supabase.table("courts").insert(payload).execute()
        return resp.data[0]["id"]
    except Exception as e:
        logging.error(f"ensure_court_exists({club_id},{name}): {e}")
        return None

def save_json(data: dict):
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    fname = JSON_DIR / f"court_data_{data['club_name'].replace(' ','_')}_{ts}.json"
    try:
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        logging.info(f"Saved JSON → {fname}")
    except Exception as e:
        logging.error(f"save_json: {e}")

def insert_into_supabase(slots: list):
    inserted = 0
    for slot in slots:
        try:
            supabase.table("slots").insert(slot).execute()
            inserted += 1
        except Exception as e:
            msg = str(e)
            if "overlaps with existing" in msg:
                continue
            logging.error(f"insert_slot: {msg}")
    if inserted:
        logging.info(f"Inserted {inserted} new slots")

# Cache for working proxies to avoid repeated testing
WORKING_PROXIES = []
PROXY_DATA_CACHE = {}

def test_proxy_with_requests(proxy_user, proxy_pass, proxy_host, proxy_port):
    """Test proxy connection using requests library"""
    url = 'https://ip.decodo.com/json'
    proxy_url = f"http://{proxy_user}:{proxy_pass}@{proxy_host}:{proxy_port}"
    
    logging.info(f"Testing proxy with requests: {proxy_url}")
    
    try:
        response = requests.get(
            url, 
            proxies={
                'http': proxy_url,
                'https': proxy_url
            },
            timeout=10
        )
        
        if response.status_code == 200:
            logging.info(f"Proxy test successful: {proxy_port}")
            try:
                # Try to parse as JSON
                data = response.json()
                return True, data
            except:
                # If not JSON, still consider it successful
                return True, response.text
        else:
            logging.warning(f"Proxy test failed with status code: {response.status_code}")
            return False, None
    except Exception as e:
        logging.error(f"Proxy request error: {str(e)}")
        return False, None

def find_working_proxy():
    """Test all proxy ports and return a working one"""
    global WORKING_PROXIES, PROXY_DATA_CACHE
    
    # If we already have working proxies, use them
    if WORKING_PROXIES:
        proxy_port = random.choice(WORKING_PROXIES)
        proxy_url = f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{proxy_port}"
        logging.info(f"Using cached working proxy: {proxy_port}")
        
        # Log the IP information for the selected proxy if available
        if proxy_port in PROXY_DATA_CACHE:
            try:
                ip_data = PROXY_DATA_CACHE[proxy_port]
                if isinstance(ip_data, dict) and 'proxy' in ip_data:
                    logging.info(f"Selected proxy: {proxy_port} - IP: {ip_data['proxy'].get('ip', 'Unknown')}")
            except:
                pass
        return proxy_url
    
    # Otherwise test all proxy ports
    logging.info("Testing proxy ports...")
    working_ports = []
    proxy_data = {}

    for port in PROXY_PORTS:
        success, data = test_proxy_with_requests(PROXY_USER, PROXY_PASS, PROXY_HOST, port)
        if success:
            working_ports.append(port)
            proxy_data[port] = data

    if working_ports:
        # Cache the working proxies for future use
        WORKING_PROXIES = working_ports
        PROXY_DATA_CACHE = proxy_data
        
        # Use a random working port
        proxy_port = random.choice(working_ports)
        proxy_url = f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{proxy_port}"
        
        # Log the IP information for the selected proxy
        if proxy_port in proxy_data:
            try:
                ip_data = proxy_data[proxy_port]
                if isinstance(ip_data, dict) and 'proxy' in ip_data:
                    logging.info(f"Selected proxy: {proxy_port} - IP: {ip_data['proxy'].get('ip', 'Unknown')}")
                    logging.info(f"Location: {ip_data.get('country', {}).get('name', 'Unknown')}, {ip_data.get('city', {}).get('name', 'Unknown')}")
            except:
                pass
        return proxy_url
    else:
        logging.error("No working proxy ports found")
        return None

def init_driver():
    # Find a working proxy
    proxy_url = find_working_proxy()
    
    if not proxy_url:
        logging.warning("Falling back to direct connection (no proxy)")
    else:
        logging.info(f"Using proxy: {proxy_url}")

    # Setup Chrome options
    opts = uc.ChromeOptions()
    opts.add_argument('--headless=new')
    opts.add_argument('--no-sandbox')
    opts.add_argument('--disable-dev-shm-usage')
    opts.add_argument('--start-maximized')
    opts.add_argument('--disable-blink-features=AutomationControlled')
    
    # Add proxy if available
    if proxy_url:
        opts.add_argument(f'--proxy-server={proxy_url}')
    
    # Add arguments to help with stability
    opts.add_argument('--disable-extensions')
    opts.add_argument('--disable-gpu')
    opts.add_argument('--disable-infobars')
    opts.add_argument('--disable-notifications')
    opts.add_argument('--disable-popup-blocking')
    
    # Use regular Selenium with webdriver_manager as a fallback
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager
    
    try:
        # First try with undetected_chromedriver
        try:
            driver = uc.Chrome(options=opts, version_main=CHROME_VERSION)
            return driver
        except Exception as e:
            logging.error(f"Failed with undetected_chromedriver: {str(e)}")
            logging.info("Falling back to regular Selenium WebDriver...")
            
            # Fall back to regular Selenium with webdriver_manager
            chrome_options = webdriver.ChromeOptions()
            chrome_options.add_argument('--headless=new')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            
            if proxy_url:
                chrome_options.add_argument(f'--proxy-server={proxy_url}')
            
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            return driver
            
    except Exception as e:
        # If everything fails, try one more time without proxy
        logging.error(f"All Chrome initialization attempts failed: {str(e)}")
        logging.info("Final attempt without proxy...")
        
        try:
            # Simple options for maximum compatibility
            chrome_options = webdriver.ChromeOptions()
            chrome_options.add_argument('--headless=new')
            chrome_options.add_argument('--no-sandbox')
            
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            return driver
        except Exception as final_e:
            logging.error(f"Final attempt failed: {str(final_e)}")
            raise

def capture_screenshot(driver, club_name):
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    fname = SCREENSHOTS_DIR / f"{club_name.replace(' ','_')}_{ts}.png"
    driver.save_screenshot(str(fname))
    logging.info(f"Screenshot → {fname}")

def get_court_availability(url, clubs, scrape_id):
    try:
        uuid_part = url.rstrip("/").split("/")[-1].split("?")[0]
        info = next((c for c in clubs if uuid_part in c["url"]), {})
        club_name = info.get("name", "unknown")
        club_id   = info.get("id")
        logging.info(f"[{club_name}] start scraping")
        
        # Initialize driver inside the try block
        try:
            driver = init_driver()
        except Exception as driver_error:
            logging.error(f"[{club_name}] Driver initialization error: {str(driver_error)}")
            return 0

        driver.get(url)
        # Increase timeout to 40 seconds
        wait = WebDriverWait(driver, 40)
        
        # Set booking date to today without needing to click any date button
        booking_date = datetime.now().strftime('%Y-%m-%d')
        logging.info(f"Using default date: {booking_date}")
        
        # Log page title to help debug
        logging.info(f"Page title: {driver.title}")
        
        # Wait for page to load completely
        time.sleep(5)
        
        try:
            # Log that we're waiting for the grid
            logging.info(f"Waiting for grid to load...")
            
            # First check if page has loaded at all
            body_text = driver.find_element(By.TAG_NAME, "body").text
            logging.info(f"Page contains text: {body_text[:100]}...")
            
            # Wait for the time banner to be visible
            wait.until(lambda d: d.find_elements(By.CSS_SELECTOR, "div[style*='grid-template-columns: 150px repeat']"))
            time.sleep(5)  # Give more time for all elements to load
            
            capture_screenshot(driver, club_name)
            
            # Find all court rows - these have the border-b class and grid-template-columns style
            court_rows = driver.find_elements(By.CSS_SELECTOR, "div.border-b[style*='grid-template-columns']")
            logging.info(f"Found {len(court_rows)} court rows")
            
            if len(court_rows) == 0:
                logging.error("No court rows found")
                # Save page source for debugging
                with open(f"page_source_{club_name.replace(' ', '_')}.html", "w", encoding="utf-8") as f:
                    f.write(driver.page_source)
                logging.info(f"Saved page source to page_source_{club_name.replace(' ', '_')}.html")
                return 0
            
            # Extract court names and available slots
            court_data = []
            
            for row in court_rows:
                try:
                    # Get the court name from the first div in the row
                    court_name_div = row.find_element(By.CSS_SELECTOR, "div.group div.truncate")
                    court_name = court_name_div.text.strip()
                    
                    if not court_name or court_name.isdigit():
                        continue  # Skip rows without proper court names
                    
                    logging.info(f"Processing court: {court_name}")
                    
                    # Find all slots in this row (those with data-start-hour attribute)
                    slots = []
                    
                    # First, find all time slots
                    slot_divs = row.find_elements(By.CSS_SELECTOR, "div[data-start-hour]")
                    logging.info(f"Found {len(slot_divs)} total time slots for {court_name}")
                    
                    # In the new format, we need to find the booked slots
                    # We'll do this by identifying slots that DON'T have the bg-white class
                    for slot in slot_divs:
                        try:
                            # Extract start and end times
                            start_time = slot.get_attribute("data-start-hour")
                            end_time = slot.get_attribute("data-end-hour")
                            
                            if not start_time or not end_time:
                                continue
                                
                            # Check if the slot has a white background (available)
                            # If it doesn't have bg-white, it's booked
                            try:
                                # Try to find bg-white class
                                slot.find_element(By.CSS_SELECTOR, "div.bg-white")
                                # If we get here, the slot is available, so we don't add it to booked slots
                                logging.debug(f"Available slot: {start_time} - {end_time}")
                            except Exception:
                                # If we can't find bg-white, it's booked
                                # Format times to match expected format
                                start_parts = start_time.split(":")
                                end_parts = end_time.split(":")
                                
                                # Handle both formats: "6:00" and "6" 
                                if len(start_parts) == 2:
                                    sh, sm = int(start_parts[0]), int(start_parts[1])
                                else:
                                    sh, sm = int(start_parts[0]), 0
                                    
                                if len(end_parts) == 2:
                                    eh, em = int(end_parts[0]), int(end_parts[1])
                                else:
                                    eh, em = int(end_parts[0]), 0
                                
                                # Add to booked slots list
                                slots.append({
                                    "start_time": f"{sh:02d}:{sm:02d}",
                                    "end_time": f"{eh:02d}:{em:02d}",
                                    "status": "Booked"
                                })
                                logging.debug(f"Booked slot: {start_time} - {end_time}")
                        except Exception as slot_error:
                            logging.error(f"Error processing slot: {str(slot_error)}")
                            continue
                    
                    court_data.append({
                        "name": court_name,
                        "slots": slots
                    })
                    
                except Exception as court_error:
                    logging.error(f"Error processing court row: {str(court_error)}")
            
            # Prepare data for structured output
            structured = {
                "club_name": club_name,
                "booking_date": booking_date,
                "scrape_timestamp": datetime.now().isoformat(),
                "courts": []
            }
            
            for court in court_data:
                # The slots are already formatted correctly in the court_data structure
                structured["courts"].append({"name": court["name"], "slots": court["slots"]})
            
            save_json(structured)
            slot_payloads = []
            for court in structured["courts"]:
                cid = ensure_court_exists(club_id, court["name"])
                if not cid:
                    continue
                for s in court["slots"]:
                    sh, sm = map(int, s["start_time"].split(":"))
                    eh, em = map(int, s["end_time"].split(":"))
                    if eh < sh:
                        eh += 24
                    dur = (eh*60+em) - (sh*60+sm)
                    
                    slot_payloads.append({
                        "court_id": cid,
                        "booking_date": structured["booking_date"],
                        "start_time": s["start_time"],
                        "end_time": s["end_time"],
                        "availability": s["status"] == "Available",
                        "duration_minutes": dur,
                        "scrape_id": scrape_id,
                        "scrape_timestamp": structured["scrape_timestamp"]
                    })

            insert_into_supabase(slot_payloads)
            logging.info(f"[{club_name}] done ({len(slot_payloads)} slots)")
            return len(slot_payloads)
            
        except Exception as e:
            logging.error(f"Error parsing grid: {str(e)}")
            logging.error(f"Grid parsing error details: {type(e).__name__}: {str(e)}")
            return 0

    except Exception as e:
        logging.error(f"[{club_name}] error: {str(e)}")
        logging.error(f"[{club_name}] error type: {type(e).__name__}")
        import traceback
        logging.error(f"[{club_name}] traceback: {traceback.format_exc()}")
        return 0
    finally:
        try:
            driver.quit()
        except:
            # Driver might not be initialized or already closed
            pass
            

                    
                    if grid_column_match:
                        start_col = int(grid_column_match.group(1))
                        end_col = int(grid_column_match.group(2))
                        
                        # Map column position to time (subtract 1 because first column is court name)
                        start_hour = time_headers[start_col - 2] if start_col - 2 < len(time_headers) else 0
                        end_hour = time_headers[end_col - 2] if end_col - 2 < len(time_headers) else 0
                        
                        # If end is less than start, it means it crosses midnight
                        if end_hour <= start_hour and end_hour != 0:
                            end_hour += 24
                            
                        slots.append({
                            "start_time": f"{start_hour:02d}:00",
                            "end_time": f"{end_hour:02d}:00",
                            "status": "Booked"
                        })
                except Exception as cell_error:
                    logging.error(f"Error processing booking cell: {str(cell_error)}")
                    continue
            structured["courts"].append({"name": name, "slots": slots})

        save_json(structured)
        slot_payloads = []
        for court in structured["courts"]:
            cid = ensure_court_exists(club_id, court["name"])
            if not cid:
                continue
            for s in court["slots"]:
                sh, sm = map(int, s["start_time"].split(":"))
                eh, em = map(int, s["end_time"].split(":"))
                if eh < sh:
                    eh += 24
                dur = (eh*60+em) - (sh*60+sm)
                slot_payloads.append({
                    "court_id": cid,
                    "booking_date": structured["booking_date"],
                    "start_time": s["start_time"],
                    "end_time": s["end_time"],
                    "availability": False,
                    "duration_minutes": dur,
                    "scrape_id": scrape_id,
                    "scrape_timestamp": structured["scrape_timestamp"]
                })

        insert_into_supabase(slot_payloads)
        logging.info(f"[{club_name}] done ({len(slot_payloads)} slots)")
        return len(slot_payloads)

    except Exception as e:
        logging.error(f"[{club_name}] error: {str(e)}")
        logging.error(f"[{club_name}] error type: {type(e).__name__}")
        import traceback
        logging.error(f"[{club_name}] traceback: {traceback.format_exc()}")
        return 0
    finally:
        try:
            driver.quit()
        except:
            # Driver might not be initialized or already closed
            pass

def scrape_all_clubs(clubs, scrape_id):
    with ThreadPoolExecutor(max_workers=2) as exec:
        futures = [exec.submit(get_court_availability, c["url"], clubs, scrape_id) for c in clubs]
        return sum(f.result() for f in futures)

def main():
    scrape_id = create_scrape_run()
    if not scrape_id:
        logging.error("Failed to create scrape run")
        return

    clubs = fetch_club_urls()
    logging.info(f"Fetched {len(clubs)} clubs")
    
    # Limit to 5 clubs for testing
    test_clubs = clubs[:5]
    logging.info(f"Testing with 5 clubs: {', '.join(c['name'] for c in test_clubs)}")

    total_slots = scrape_all_clubs(test_clubs, scrape_id)
    update_scrape_run(scrape_id, "completed", total_slots, len(test_clubs))

    logging.info(f"Completed scrape_run={scrape_id}: {total_slots} slots across {len(test_clubs)} clubs")

if __name__ == "__main__":
    main()
