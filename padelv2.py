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

# Load environment
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_club_urls():
    try:
        resp = supabase.table("clubs").select("id, name, url").execute()
        return resp.data or []
    except Exception as e:
        print(f"❌ Error fetching clubs: {e}")
        return []

def get_court_availability(url, clubs_data, scrape_id):
    driver = None
    try:
        uuid_from_url = url.split('/')[-1].split('?')[0]
        club_info = next((c for c in clubs_data if uuid_from_url in c['url']), None)
        club_name = club_info['name'] if club_info else "Unknown Club"
        club_id = club_info['id'] if club_info else None
        print(f"🔎 Scraping: {club_name}")

        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--single-process')
        options.add_argument('--window-size=1920,1080')
        options.add_experimental_option('excludeSwitches', ['enable-logging'])

        driver = webdriver.Chrome(options=options)
        driver.get(url)

        wait = WebDriverWait(driver, 15)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#root .bbq2__grid")))
        time.sleep(2)

        booking_date = datetime.now().strftime('%Y-%m-%d')
        grid = driver.find_element(By.CSS_SELECTOR, "#root .bbq2__grid")
        court_names = [c.text.strip() for c in grid.find_elements(By.CLASS_NAME, "bbq2__resource__label")]
        slots_resources = grid.find_elements(By.CLASS_NAME, "bbq2__slots-resource")

        structured_data = {
            "club_name": club_name,
            "booking_date": booking_date,
            "scrape_timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "courts": []
        }

        grid_start_offset = 350
        pixels_per_hour = 39
        calibration = +1.0 if any(x in club_name for x in ["Playmore", "Playpadelhartbeespoort", "Lynnwood Glen"]) else 0.0

        for idx, court_name in enumerate(court_names):
            if idx >= len(slots_resources):
                continue
            slots = slots_resources[idx].find_elements(By.XPATH, ".//div[contains(@class, 'bbq2__hole')]")
            court_slots = []

            for slot in slots:
                x = slot.location['x']
                width = slot.size['width']

                start_hour = (x - grid_start_offset) / pixels_per_hour + calibration
                end_hour = (x + width - grid_start_offset) / pixels_per_hour + calibration

                sh, sm = divmod(int(start_hour * 60), 60)
                eh, em = divmod(int(end_hour * 60), 60)

                sh, sm = max(sh, 6), sm
                eh, em = min(eh, 23), em

                court_slots.append({
                    'start_time': f"{sh:02d}:{sm:02d}",
                    'end_time': f"{eh:02d}:{em:02d}",
                    'status': 'Booked'
                })

            structured_data["courts"].append({
                "name": court_name,
                "slots": court_slots
            })

        upload_json_to_supabase(structured_data)
        slots_data = prepare_slots_data(structured_data, club_id, booking_date, scrape_id)

        if slots_data:
            insert_into_supabase(slots_data)

        return len(slots_data)

    except Exception as e:
        print(f"❌ Error scraping {url}: {e}")
        return 0
    finally:
        if driver:
            driver.quit()

def prepare_slots_data(structured_data, club_id, booking_date, scrape_id):
    slots = []
    for court in structured_data["courts"]:
        court_id = ensure_court_exists(club_id, court["name"])
        if not court_id:
            continue
        for s in court["slots"]:
            sh, sm = map(int, s["start_time"].split(":"))
            eh, em = map(int, s["end_time"].split(":"))
            duration = (eh - sh) * 60 + (em - sm)
            slots.append({
                'court_id': court_id,
                'booking_date': booking_date,
                'start_time': s["start_time"],
                'end_time': s["end_time"],
                'availability': False,
                'duration_minutes': duration,
                'scrape_id': scrape_id,
                'scrape_timestamp': structured_data["scrape_timestamp"]
            })
    return slots

def upload_json_to_supabase(structured_data):
    try:
        bucket = "scraped-json"
        today_folder = datetime.now().strftime('%Y-%m-%d')
        path = f"{today_folder}/courts_{structured_data['club_name'].replace(' ', '_')}.json"
        json_bytes = json.dumps(structured_data, indent=4).encode('utf-8')

        supabase.storage.from_(bucket).upload(
            path=path,
            file=json_bytes,
            options={"content-type": "application/json", "cache-control": "3600", "upsert": True}
        )
        print(f"✅ Uploaded JSON to {bucket}/{path}")

    except Exception as e:
        print(f"❌ JSON Upload Error: {e}")

def insert_into_supabase(slots_data):
    try:
        if not slots_data:
            return
        batch_size = 100
        total = len(slots_data)
        inserted, failed = 0, 0
        print(f"Inserting {total} slots...")
        for i in range(0, total, batch_size):
            batch = slots_data[i:i+batch_size]
            try:
                resp = supabase.table("slots").insert(batch).execute()
                if resp.data:
                    inserted += len(resp.data)
            except Exception as e:
                print(f"Insert batch error: {e}")
                failed += len(batch)
        print(f"✔️ {inserted} inserted, ❌ {failed} failed.")
    except Exception as e:
        print(f"Fatal insert error: {e}")

def ensure_court_exists(club_id, court_name):
    try:
        if not club_id:
            return None
        existing = supabase.table("courts").select("*").eq("club_id", club_id).eq("name", court_name).execute()
        if existing.data:
            return existing.data[0]["id"]
        new = supabase.table("courts").insert({"club_id": club_id, "name": court_name, "created_at": datetime.now().isoformat()}).execute()
        return new.data[0]["id"] if new.data else None
    except Exception as e:
        print(f"Court exists error: {e}")
        return None

def create_scrape_run():
    try:
        run = {
            "run_at": datetime.now().isoformat(),
            "booking_date": datetime.now().strftime('%Y-%m-%d'),
            "source": "padelv2.py",
            "notes": "Automated scrape",
            "slots_scraped": 0,
            "clubs_covered": 0,
            "scrape_status": "in_progress"
        }
        resp = supabase.table("scrape_runs").insert(run).execute()
        return resp.data[0]["id"] if resp.data else None
    except Exception as e:
        print(f"Scrape run error: {e}")
        return None

def update_scrape_run(scrape_id, status, slots, clubs):
    try:
        supabase.table("scrape_runs").update({
            "scrape_status": status,
            "slots_scraped": slots,
            "clubs_covered": clubs
        }).eq("id", scrape_id).execute()
    except Exception as e:
        print(f"Update scrape run error: {e}")

def scrape_all_clubs(club_data, scrape_id):
    results = {}
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(get_court_availability, c["url"], club_data, scrape_id): c["name"] for c in club_data}
        for f in futures:
            club_name = futures[f]
            try:
                results[club_name] = f.result()
            except Exception as e:
                print(f"Club scrape error {club_name}: {e}")
                results[club_name] = 0
    return results

def main():
    scrape_id = create_scrape_run()
    if not scrape_id:
        print("Failed to create scrape run.")
        return

    clubs = fetch_club_urls()
    print(f"📋 {len(clubs)} clubs fetched.")

    results = scrape_all_clubs(clubs, scrape_id)
    total_slots = sum(results.values())

    update_scrape_run(scrape_id, "completed", total_slots, len(clubs))
    print(f"✅ Scrape completed: {total_slots} slots across {len(clubs)} clubs.")

if __name__ == "__main__":
    main()
