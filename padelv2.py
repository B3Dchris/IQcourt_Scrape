from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import json
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from supabase import create_client
from dotenv import load_dotenv
import math

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

print("Supabase URL:", SUPABASE_URL)
print("Supabase Key:", (SUPABASE_KEY[:5] + "..." if SUPABASE_KEY else "Not found"))

def fetch_club_urls():
    try:
        response = supabase.table("clubs").select("id, name, url").execute()
        return response.data if response.data else []
    except Exception as e:
        print(f"Exception fetching clubs: {str(e)}")
        return []

def get_court_availability(url, clubs_data, scrape_id):
    driver = None
    try:
        uuid_from_url = url.split('/')[-1].split('?')[0]
        club_info = next((club for club in clubs_data if uuid_from_url in club['url']), None)

        club_name = club_info['name'] if club_info else "Unknown Club"
        club_id = club_info['id'] if club_info else None
        print(f"Scraping: {club_name}")

        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--start-maximized')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-infobars')
        options.add_argument('--window-size=1920,1080')
        driver = webdriver.Chrome(options=options)
        driver.get(url)

        wait = WebDriverWait(driver, 15)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#root .bbq2__grid")))
        time.sleep(2)

        booking_date = datetime.now().strftime('%Y-%m-%d')
        grid_element = driver.find_element(By.CSS_SELECTOR, "#root .bbq2__grid")
        court_names = [c.text.strip() for c in grid_element.find_elements(By.CLASS_NAME, "bbq2__resource__label")]
        slots_resources = grid_element.find_elements(By.CLASS_NAME, "bbq2__slots-resource")

        structured_data = {
            "club_name": club_name,
            "booking_date": booking_date,
            "scrape_timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "courts": []
        }

        grid_start_offset = 350
        pixels_per_hour = 39
        calibration = +1.0 if any(x in club_name for x in ["Playmore", "Playpadelhartbeespoort", "Lynnwood Glen"]) else 0.0

        for court_index, court_name in enumerate(court_names):
            if court_index >= len(slots_resources):
                continue

            slots = slots_resources[court_index].find_elements(By.XPATH, ".//div[contains(@class, 'bbq2__hole')]")
            court_slots = []

            for slot in slots:
                x = slot.location['x']
                width = slot.size['width']
                start_hour = (x - grid_start_offset) / pixels_per_hour + calibration
                end_hour = (x + width - grid_start_offset) / pixels_per_hour + calibration

                start_h, start_m = divmod(round(start_hour * 60), 60)
                end_h, end_m = divmod(round(end_hour * 60), 60)

                if start_h < 6: start_h, start_m = 6, 0
                if end_h > 23: end_h, end_m = 23, 59

                court_slots.append({
                    'start_time': f"{start_h:02d}:{start_m:02d}",
                    'end_time': f"{end_h:02d}:{end_m:02d}",
                    'status': 'Booked'
                })

            structured_data["courts"].append({
                'name': court_name,
                'slots': court_slots
            })

        save_json(structured_data)
        slots_data = prepare_slots_data(structured_data, club_id, booking_date, scrape_id)
        if slots_data:
            insert_into_supabase(slots_data)

        return len(slots_data)

    except Exception as e:
        print(f"Error scraping {url}: {str(e)}")
        return 0
    finally:
        if driver:
            driver.quit()

def prepare_slots_data(structured_data, club_id, booking_date, scrape_id):
    slots_data = []
    for court in structured_data["courts"]:
        court_id = ensure_court_exists(club_id, court['name'])
        if not court_id:
            continue
        for slot in court['slots']:
            sh, sm = map(int, slot['start_time'].split(":"))
            eh, em = map(int, slot['end_time'].split(":"))
            duration = (eh - sh) * 60 + (em - sm)
            slots_data.append({
                'court_id': court_id,
                'booking_date': booking_date,
                'start_time': slot['start_time'],
                'end_time': slot['end_time'],
                'availability': False,
                'duration_minutes': duration,
                'scrape_id': scrape_id,
                'scrape_timestamp': structured_data['scrape_timestamp']
            })
    return slots_data

def save_json(structured_data):
    try:
        # Prepare the JSON content in memory
        combined_data = {
            "booking_date": structured_data['booking_date'],
            "scrape_timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "clubs": [
                {
                    "club_name": structured_data['club_name'],
                    "courts": structured_data['courts']
                }
            ]
        }

        # Convert JSON to bytes
        json_bytes = json.dumps(combined_data, indent=4).encode('utf-8')

        # Prepare upload path like 2025-04-28/courts.json
        upload_path = f"{datetime.now().strftime('%Y-%m-%d')}/courts.json"

        # Upload to Supabase Storage
        supabase.storage.from_("scraped-json").upload(
            path=upload_path,
            file=json_bytes,
            options={"content-type": "application/json", "cache-control": "3600", "upsert": True}
        )

        print(f"✅ Uploaded JSON directly to Supabase Storage: scraped-json/{upload_path}")

    except Exception as e:
        print(f"❌ Error uploading JSON: {str(e)}")


def insert_into_supabase(slots_data):
    try:
        if not slots_data:
            return
        batch_size = 100
        total, success, fail = len(slots_data), 0, 0

        print(f"Inserting {total} slots...")
        for i in range(0, total, batch_size):
            batch = slots_data[i:i + batch_size]
            try:
                resp = supabase.table("slots").insert(batch).execute()
                if resp.data:
                    success += len(resp.data)
            except Exception as e:
                print(f"Insert batch error: {str(e)}")
                fail += len(batch)

        print(f"✔️ {success} inserted, ❌ {fail} failed.")

    except Exception as e:
        print(f"Fatal error inserting to Supabase: {str(e)}")

def ensure_court_exists(club_id, court_name):
    try:
        if not club_id:
            return None
        existing = supabase.table("courts").select("*").eq("club_id", club_id).eq("name", court_name).execute()
        if existing.data:
            return existing.data[0]['id']
        new = supabase.table("courts").insert({'club_id': club_id, 'name': court_name, 'created_at': datetime.now().isoformat()}).execute()
        return new.data[0]['id'] if new.data else None
    except Exception as e:
        print(f"Error ensuring court: {str(e)}")
        return None

def create_scrape_run():
    try:
        data = {
            'run_at': datetime.now().isoformat(),
            'booking_date': datetime.now().strftime('%Y-%m-%d'),
            'source': 'padelv2.py',
            'notes': 'Automated playtomic scrape',
            'slots_scraped': 0,
            'clubs_covered': 0,
            'scrape_status': 'in_progress'
        }
        resp = supabase.table("scrape_runs").insert(data).execute()
        return resp.data[0]['id'] if resp.data else None
    except Exception as e:
        print(f"Error creating scrape run: {str(e)}")
        return None

def update_scrape_run(scrape_id, status, slots, clubs):
    try:
        data = {'scrape_status': status, 'slots_scraped': slots, 'clubs_covered': clubs}
        supabase.table("scrape_runs").update(data).eq('id', scrape_id).execute()
        return True
    except Exception as e:
        print(f"Error updating scrape run: {str(e)}")
        return False

def scrape_all_clubs(club_data, scrape_id):
    results = {}
    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = {ex.submit(get_court_availability, club['url'], club_data, scrape_id): club['name'] for club in club_data}
        for future in futures:
            club_name = futures[future]
            try:
                results[club_name] = future.result()
            except Exception as e:
                print(f"Error on {club_name}: {str(e)}")
                results[club_name] = 0
    return results

def main():
    scrape_id = create_scrape_run()
    if not scrape_id:
        print("Failed to create scrape run.")
        return

    clubs = fetch_club_urls()
    print(f"Fetched {len(clubs)} clubs.")

    results = scrape_all_clubs(clubs, scrape_id)
    total_slots = sum(results.values())

    update_scrape_run(scrape_id, 'completed', total_slots, len(clubs))
    print(f"Done. Scraped {total_slots} slots from {len(clubs)} clubs.")

if __name__ == "__main__":
    main()
