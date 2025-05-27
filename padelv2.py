import os
import time
import uuid
import json
import logging
import argparse
from datetime import datetime, timezone, timedelta, time as dtime
from dotenv import load_dotenv
from supabase import create_client
from playwright.sync_api import sync_playwright

# --- Setup ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- Supabase Functions ---
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
    try:
        supabase.table("scrape_runs").insert(payload).execute()
        logger.info("Created scrape run %s", scrape_id)
    except Exception as e:
        logger.error("Error creating scrape run: %s", e)
    return scrape_id

def fetch_clubs():
    res = supabase.table("clubs").select("id,name,url").execute()
    return res.data if res.data else []

def ensure_court_exists(club_id, court_name):
    try:
        query = supabase.table("courts").select("id").eq("club_id", club_id).eq("name", court_name).execute()
        if query.data:
            return query.data[0]["id"]
        court_id = str(uuid.uuid4())
        supabase.table("courts").insert({"id": court_id, "club_id": club_id, "name": court_name}).execute()
        return court_id
    except Exception as e:
        logger.error("Error ensuring court exists: %s", e)
        return None

def replace_slots_for_day(booking_date):
    time.sleep(2)
    try:
        supabase.table("slots").delete().eq("booking_date", booking_date).execute()
        logger.info("Deleted existing slots for %s", booking_date)
    except Exception as e:
        logger.error("Failed to delete existing slots: %s", e)

def insert_slots(slots, booking_date):
    if not slots:
        logger.warning("No slots to insert")
        return

    available_slots = [slot for slot in slots if slot.get("availability") is True]
    logger.info("Filtered %d slots to %d available slots", len(slots), len(available_slots))

    if not available_slots:
        logger.warning("No available slots to insert")
        return

    with open(f"slots_debug_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", "w") as f:
        json.dump(available_slots, f, indent=2)

    replace_slots_for_day(booking_date)
    try:
        supabase.table("slots").insert(available_slots).execute()
        logger.info("Inserted %d available slots for %s", len(available_slots), booking_date)
    except Exception as e:
        logger.error("Error inserting slots: %s", e)
        logger.info("Attempting slot-by-slot insert...")
        success_count = 0
        for slot in available_slots:
            try:
                supabase.table("slots").insert([slot]).execute()
                success_count += 1
            except Exception as se:
                logger.error("Slot insert failed: %s", se)
        logger.info("Successfully inserted %d/%d slots", success_count, len(available_slots))

def dedupe_slots(slots):
    grouped = {}
    for slot in slots:
        key = (slot["court_id"], slot["booking_date"])
        grouped.setdefault(key, []).append(slot)

    result = []
    for (court_id, date), court_slots in grouped.items():
        court_slots.sort(key=lambda x: x["start_time"])
        merged = []
        for slot in court_slots:
            if not merged:
                merged.append(slot)
            else:
                prev = merged[-1]
                if slot["start_time"] <= prev["end_time"]:
                    prev["end_time"] = max(prev["end_time"], slot["end_time"])
                    start_dt = datetime.strptime(prev["start_time"], "%H:%M:%S")
                    end_dt = datetime.strptime(prev["end_time"], "%H:%M:%S")
                    prev["duration_minutes"] = int((end_dt - start_dt).total_seconds() / 60)
                else:
                    merged.append(slot)
        result.extend(merged)

    logger.info("Deduplicated slots from %d to %d", len(slots), len(result))
    return result

def extract_courts(club, scrape_id, booking_date):
    from datetime import datetime as dt

    club_id = club["id"]
    url = club["url"]
    slots_data = []

    logger.info("Scraping %s", url)

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, timeout=60000)
            page.wait_for_selector("div.border-b")

            court_rows = page.query_selector_all("div.border-b")
            logger.info("Found %d court rows for club: %s", len(court_rows), club["name"])

            for index, row in enumerate(court_rows):
                try:
                    court_name = f"Court {index+1}"
                    name_el = row.query_selector("div.font-medium")
                    if name_el:
                        court_name = name_el.inner_text().strip() or court_name

                    court_id = ensure_court_exists(club_id, court_name)
                    if not court_id:
                        continue

                    slot_elements = row.query_selector_all("div[data-start-hour][data-end-hour]")
                    fmt = "%H:%M"

                    for el in slot_elements:
                        start_time = el.get_attribute("data-start-hour")
                        end_time = el.get_attribute("data-end-hour")
                        start = dt.strptime(start_time, fmt).time()
                        end = dt.strptime(end_time, fmt).time()

                        start_dt = dt.combine(dt.today(), start)
                        end_dt = dt.combine(dt.today(), end)
                        duration = int((end_dt - start_dt).total_seconds() / 60)

                        slots_data.append({
                            "court_id": court_id,
                            "booking_date": booking_date,
                            "start_time": start.strftime("%H:%M:%S"),
                            "end_time": end.strftime("%H:%M:%S"),
                            "duration_minutes": duration,
                            "availability": True,
                            "scrape_id": scrape_id,
                            "scrape_timestamp": datetime.now(timezone.utc).isoformat()
                        })
                except Exception as e:
                    logger.warning("Error on court row %s: %s", index, e)

            browser.close()

    except Exception as e:
        logger.error("Playwright error for club %s: %s", club.get("name", "Unknown"), e)

    logger.info("Extracted %d available slots", len(slots_data))
    return slots_data

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    scrape_id = create_scrape_run()
    booking_date = datetime.now().date().isoformat()
    clubs = fetch_clubs()

    logger.info("Found %d clubs", len(clubs))
    to_process = clubs[:args.limit] if args.limit else clubs
    logger.info("Processing %d clubs", len(to_process))

    total_slots = []
    for club in to_process:
        logger.info("Processing club: %s (%s)", club["name"], club["url"])
        total_slots += extract_courts(club, scrape_id, booking_date)

    total_slots = dedupe_slots(total_slots)
    logger.info("Total slots before insert: %d", len(total_slots))
    insert_slots(total_slots, booking_date)
    logger.info("Scraper finished successfully.")

if __name__ == "__main__":
    main()
