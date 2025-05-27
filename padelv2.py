import os
import time
import uuid
import logging
import argparse
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta, time as dtime
from supabase import create_client
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from undetected_chromedriver import Chrome, ChromeOptions

# --- Setup ---
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- Init Chrome ---
def init_driver():
    options = ChromeOptions()
    options.add_argument('--headless=new')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-blink-features=AutomationControlled')
    
    chrome_bin = "/usr/bin/google-chrome"
    if os.path.exists(chrome_bin):
        options.binary_location = chrome_bin  # only set if exists
    else:
        logger.warning("Chrome binary not found at %s; using default binary", chrome_bin)

    return Chrome(options=options)

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
    time.sleep(2)  # Add a small delay to ensure delete completes
    try:
        supabase.table("slots").delete().eq("booking_date", booking_date).execute()
        logger.info("Deleted existing slots for %s", booking_date)
    except Exception as e:
        logger.error("Failed to delete existing slots: %s", e)

def insert_slots(slots, booking_date):
    if not slots:
        logger.warning("No slots to insert")
        return
    
    # Filter to only include available slots
    available_slots = [slot for slot in slots if slot.get("availability", False) == True]
    logger.info("Filtered %d slots to %d available slots", len(slots), len(available_slots))
    
    if not available_slots:
        logger.warning("No available slots to insert")
        return
    
    # Add detailed logging before insert
    logger.info("=== DETAILED SLOT ANALYSIS ===")
    
    # Group slots by court to identify potential overlaps
    courts = {}
    for slot in available_slots:
        court_id = slot["court_id"]
        if court_id not in courts:
            courts[court_id] = []
        courts[court_id].append(slot)
    
    # Sort slots by start time for each court and look for overlaps
    for court_id, court_slots in courts.items():
        court_slots.sort(key=lambda x: x["start_time"])
        logger.info(f"Court ID: {court_id} - {len(court_slots)} slots")
        
        # Check for potential overlaps
        for i in range(len(court_slots) - 1):
            curr = court_slots[i]
            next_slot = court_slots[i + 1]
            
            # Check if this slot ends after the next one starts
            if curr["end_time"] > next_slot["start_time"]:
                logger.warning(f"POTENTIAL OVERLAP: Court {court_id}")
                logger.warning(f"  Slot 1: {curr['start_time']} to {curr['end_time']}")
                logger.warning(f"  Slot 2: {next_slot['start_time']} to {next_slot['end_time']}")
    
    # Export all slots to a JSON file for investigation
    import json
    with open(f"slots_debug_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", "w") as f:
        json.dump(available_slots, f, indent=2)
    logger.info(f"Exported {len(available_slots)} available slots to JSON for debugging")
    
    # Continue with normal operation
    replace_slots_for_day(booking_date)
    try:
        supabase.table("slots").insert(available_slots).execute()
        logger.info("Inserted %d available slots for %s", len(available_slots), booking_date)
    except Exception as e:
        if hasattr(e, 'response') and hasattr(e.response, 'json'):
            logger.error("Insert error detail: %s", e.response.json())
        logger.error("Error inserting slots: %s", e)
        
        # Try inserting one by one to identify problem slots
        logger.info("Attempting slot-by-slot insert to identify problematic slots...")
        success_count = 0
        for i, slot in enumerate(available_slots):
            try:
                supabase.table("slots").insert([slot]).execute()
                success_count += 1
            except Exception as slot_error:
                logger.error("Error inserting slot %d: %s", i, slot)
                if hasattr(slot_error, 'response') and hasattr(slot_error.response, 'json'):
                    logger.error("Error details: %s", slot_error.response.json())
        
        logger.info("Successfully inserted %d/%d slots individually", success_count, len(available_slots))

def dedupe_slots(slots):
    """
    Deduplicates slots and merges overlapping slots for the same court on the same date.
    """
    # Group slots by court_id and booking_date
    grouped_slots = {}
    for slot in slots:
        key = (slot["court_id"], slot["booking_date"])
        if key not in grouped_slots:
            grouped_slots[key] = []
        grouped_slots[key].append(slot)
    
    result = []
    for (court_id, booking_date), court_slots in grouped_slots.items():
        # Sort slots by start_time
        court_slots.sort(key=lambda x: x["start_time"])
        
        # Merge overlapping slots
        merged = []
        for slot in court_slots:
            # Convert times to comparable format
            start_time = slot["start_time"]
            end_time = slot["end_time"]
            
            if not merged:
                # First slot
                merged.append(slot)
            else:
                prev_slot = merged[-1]
                prev_end = prev_slot["end_time"]
                
                # Check if current slot overlaps with previous slot
                if start_time <= prev_end:
                    # Slots overlap, merge them by taking the later end time
                    if end_time > prev_end:
                        # Update duration in minutes
                        from datetime import datetime
                        start_dt = datetime.strptime(prev_slot["start_time"], "%H:%M:%S")
                        end_dt = datetime.strptime(end_time, "%H:%M:%S")
                        duration = int((end_dt - start_dt).total_seconds() / 60)
                        
                        # Update the previous slot with the new end time and duration
                        prev_slot["end_time"] = end_time
                        prev_slot["duration_minutes"] = duration
                else:
                    # No overlap, add as a new slot
                    merged.append(slot)
        
        # Add the merged slots to the result
        result.extend(merged)
    
    logger.info(f"Deduplicated slots from {len(slots)} to {len(result)} after merging overlaps")
    return result

def merge_overlapping_intervals(intervals):
    intervals = sorted(intervals, key=lambda x: x[0])
    merged = []
    for start, end in intervals:
        if not merged or start > merged[-1][1]:
            merged.append((start, end))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
    return merged

def extract_courts(club, scrape_id, booking_date):
    from datetime import datetime as dt

    url = club["url"]
    club_id = club["id"]
    slots_data = []
    logger.info("Scraping %s", url)

    try:
        with init_driver() as driver:
            driver.get(url)
            time.sleep(5)
            
            # Log the HTML content for debugging
            page_html = driver.page_source
            with open(f"debug_html_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html", "w", encoding="utf-8") as f:
                f.write(page_html)
            logger.info(f"Saved HTML content to debug_html_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html")
            
            rows = driver.find_elements(By.CSS_SELECTOR, "div.border-b")
            logger.info("Found %d court rows for club: %s", len(rows), club["name"])

            for index, row in enumerate(rows):
                try:
                    court_name = f"Court {index+1}"
                    try:
                        court_name_el = row.find_element(By.CSS_SELECTOR, "div.font-medium")
                        court_name = court_name_el.text.strip() or court_name
                    except NoSuchElementException:
                        pass

                    court_id = ensure_court_exists(club_id, court_name)
                    if not court_id:
                        continue

                    slot_elements = row.find_elements(By.CSS_SELECTOR, "div[data-start-hour][data-end-hour]")
                    fmt = "%H:%M"

                    # Create available slots
                    for el in slot_elements:
                        start_time = el.get_attribute("data-start-hour")
                        end_time = el.get_attribute("data-end-hour")
                        start = dt.strptime(start_time, fmt).time()
                        end = dt.strptime(end_time, fmt).time()
                        
                        # Calculate duration in minutes
                        start_dt = dt.combine(dt.today(), start)
                        end_dt = dt.combine(dt.today(), end)
                        duration = int((end_dt - start_dt).total_seconds() / 60)
                        
                        # Log the slot element details
                        logger.info(f"Found available slot: {court_name}, {start_time}-{end_time}, class: {el.get_attribute('class')}")
                        
                        # Create the available slot
                        slots_data.append({
                            "court_id": court_id,
                            "booking_date": booking_date,
                            "start_time": start.strftime("%H:%M:%S"),
                            "end_time": end.strftime("%H:%M:%S"),
                            "duration_minutes": duration,
                            "availability": True,  # This slot is available
                            "scrape_id": scrape_id,
                            "scrape_timestamp": datetime.now(timezone.utc).isoformat()
                        })

                except Exception as e:
                    logger.warning("Error on court row %s: %s", index, e)
                    logger.exception("Full traceback for court row error:")

    except Exception as e:
        logger.error("Driver failure for %s: %s", club.get("name", "Unknown"), e)
        logger.exception("Full traceback for driver failure:")

    logger.info(f"Extracted {len(slots_data)} available slots")
    return slots_data

# --- Main ---
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Number of clubs to process (default: all)")
    args = parser.parse_args()

    scrape_id = create_scrape_run()
    booking_date = datetime.now().date().isoformat()
    clubs = fetch_clubs()
    logger.info("Found %d clubs", len(clubs))

    total_slots = []
    # Process all clubs if limit is None, otherwise respect the limit
    clubs_to_process = clubs[:args.limit] if args.limit else clubs
    logger.info("Processing %d clubs", len(clubs_to_process))
    
    for club in clubs_to_process:
        logger.info("Processing club: %s (%s)", club["name"], club["url"])
        total_slots += extract_courts(club, scrape_id, booking_date)

    total_slots = dedupe_slots(total_slots)
    logger.info("Total slots before insert: %d", len(total_slots))
    insert_slots(total_slots, booking_date)
    logger.info("Scraper finished successfully.")

if __name__ == "__main__":
    main()
