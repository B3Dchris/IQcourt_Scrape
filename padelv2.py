import os
import time
import uuid
import logging
import argparse
from datetime import datetime, timezone
from dotenv import load_dotenv
from supabase import create_client
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Setup
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def create_scrape_run():
    scrape_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    payload = {
        "id": scrape_id,
        "run_at": now,
        "start_time": now,
        "status": "running",
        "total_slots": 0,
        "booking_date": datetime.now().date().isoformat()
    }
    supabase.table("scrape_runs").insert(payload).execute()
    logger.info("Created scrape run %s", scrape_id)
    return scrape_id

def fetch_clubs():
    res = supabase.table("clubs").select("id,name,url").execute()
    return res.data if res.data else []

def ensure_court_exists(club_id, court_name):
    query = supabase.table("courts").select("id").eq("club_id", club_id).eq("name", court_name).execute()
    if query.data:
        return query.data[0]["id"]
    court_id = str(uuid.uuid4())
    supabase.table("courts").insert({"id": court_id, "club_id": club_id, "name": court_name}).execute()
    return court_id

def insert_slots(slots, booking_date):
    if not slots:
        logger.warning("No slots to insert")
        return
    try:
        supabase.table("slots").insert(slots).execute()
        logger.info("Inserted %d slots for %s", len(slots), booking_date)
    except Exception as e:
        logger.error("Bulk insert failed: %s", e)
        for slot in slots:
            try:
                supabase.table("slots").insert([slot]).execute()
            except Exception as ex:
                logger.error("Single insert failed: %s", ex)

def init_driver():
    options = webdriver.ChromeOptions()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920,1080')
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

def scrape_club(club, scrape_id, booking_date):
    driver = init_driver()
    slots = []
    try:
        logger.info("Scraping %s", club["url"])
        driver.get(club["url"])
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.border-b")))

        court_rows = driver.find_elements(By.CSS_SELECTOR, "div.border-b")
        logger.info("Found %d court rows", len(court_rows))

        for index, row in enumerate(court_rows):
            try:
                court_name = f"Court {index+1}"
                name_el = row.find_element(By.CSS_SELECTOR, "div.font-medium")
                court_name = name_el.text.strip() or court_name

                court_id = ensure_court_exists(club["id"], court_name)
                slot_elements = row.find_elements(By.CSS_SELECTOR, "div[data-start-hour][data-end-hour]")

                for el in slot_elements:
                    start = el.get_attribute("data-start-hour")
                    end = el.get_attribute("data-end-hour")
                    slots.append({
                        "court_id": court_id,
                        "booking_date": booking_date,
                        "start_time": f"{start}:00",
                        "end_time": f"{end}:00",
                        "duration_minutes": (int(end.split(":")[0]) - int(start.split(":")[0])) * 60,
                        "availability": True,
                        "scrape_id": scrape_id,
                        "scrape_timestamp": datetime.now(timezone.utc).isoformat()
                    })

            except Exception as e:
                logger.warning("Court row error: %s", e)

    except Exception as e:
        logger.error("Failed to scrape %s: %s", club["name"], e)
    finally:
        driver.quit()
    return slots

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    scrape_id = create_scrape_run()
    booking_date = datetime.now().date().isoformat()
    clubs = fetch_clubs()
    logger.info("Found %d clubs", len(clubs))
    to_process = clubs[:args.limit] if args.limit else clubs

    all_slots = []
    for club in to_process:
        all_slots.extend(scrape_club(club, scrape_id, booking_date))

    logger.info("Total extracted slots: %d", len(all_slots))
    insert_slots(all_slots, booking_date)
    logger.info("Scraping completed.")

if __name__ == "__main__":
    main()
