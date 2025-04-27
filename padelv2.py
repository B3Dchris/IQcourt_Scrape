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
import uuid
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

        if club_info:
            club_name = club_info['name']
            club_id = club_info['id']
            print(f"Found club: {club_name} with ID: {club_id}")
        else:
            club_name = "Unknown Club"
            club_id = None
            print(f"Warning: Could not find club info for URL: {url}")

        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--start-maximized')
        options.add_argument('--log-level=3')
        options.add_experimental_option('excludeSwitches', ['enable-logging'])

        driver = webdriver.Chrome(options=options)
        driver.get(url)
        wait = WebDriverWait(driver, 10)

        # Wait for the booking date element
        wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, 
            "#root > div > div.page > div.page__body > div.page__content > div > div.new_tenant__body > div.new_tenant__main > div:nth-child(1) > div > div.bbq2__search > div:nth-child(2) > div > div > button > span.bbq2__drop__toggle__label")))
        
        booking_date = datetime.now().strftime('%Y-%m-%d')

        # Wait for grid to load
        wait.until(EC.presence_of_element_located((By.XPATH, 
            "//*[@id='root']/div/div[2]/div[2]/div[1]/div/div[3]/div[1]/div[1]")))
        time.sleep(10)

        grid_element = driver.find_element(By.CSS_SELECTOR, 
            "#root > div > div.page > div.page__body > div.page__content > div > div.new_tenant__body > div.new_tenant__main > div:nth-child(1) > div > div.bbq2__grid")

        court_names = grid_element.find_elements(By.CLASS_NAME, "bbq2__resource__label")
        court_names_text = [court.text.strip() for court in court_names]

        # Grid parameters
        grid_start_offset = 350
        pixels_per_hour = 39

        # Club-specific time calibration
        if "Playmore" in club_name:
            time_calibration_offset = +1.0
        elif "Playpadelhartbeespoort" in club_name:
            time_calibration_offset = +1.0
        elif "Lynnwood Glen" in club_name:
            time_calibration_offset = +1.0
        elif "Moove Motion Fitness Club Sunninghill" in club_name:
            time_calibration_offset = 0.0
        else:
            time_calibration_offset = -1.0

        availability_slots_resources = grid_element.find_elements(By.CLASS_NAME, "bbq2__slots-resource")

        structured_data = {
            "club_name": club_name,
            "booking_date": booking_date,
            "scrape_timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "courts": []
        }

        for court_index, court_name in enumerate(court_names_text):
            if court_index < len(availability_slots_resources):
                slots_resource = availability_slots_resources[court_index]
                slots = slots_resource.find_elements(By.XPATH, ".//div[contains(@class, 'bbq2__hole')]")
                
                court_availability = []
                for slot in slots:
                    slot_position = slot.location['x']
                    slot_width = slot.size['width']

                    start_pos_relative = slot_position - grid_start_offset
                    start_hour_exact = start_pos_relative / pixels_per_hour + time_calibration_offset

                    start_hour_floor = math.floor(start_hour_exact)
                    start_minute = 30 if start_hour_exact - start_hour_floor >= 0.5 else 0

                    if start_hour_floor < 6:
                        start_hour_floor = 6
                        start_minute = 0
                    elif start_hour_floor >= 23 and start_minute > 0:
                        start_hour_floor = 23
                        start_minute = 30
                    elif start_hour_floor > 23:
                        start_hour_floor = 23
                        start_minute = 30

                    start_time = f"{start_hour_floor:02d}:{start_minute:02d}"

                    end_pos_relative = start_pos_relative + slot_width
                    end_hour_exact = end_pos_relative / pixels_per_hour + time_calibration_offset

                    end_hour_floor = math.floor(end_hour_exact)
                    end_minute = 30 if end_hour_exact - end_hour_floor >= 0.5 else 0

                    actual_end_hour = end_hour_floor % 24
                    end_time = f"{actual_end_hour:02d}:{end_minute:02d}"

                    court_availability.append({
                        'start_time': start_time,
                        'end_time': end_time,
                        'status': 'Booked'
                    })

                structured_data["courts"].append({
                    'name': court_name,
                    'slots': court_availability
                })

        save_json(structured_data)
        slots_data = prepare_slots_data(structured_data, club_id, booking_date, scrape_id)

        if slots_data:
            insert_into_supabase(slots_data)

        return len(slots_data)

    except Exception as e:
        print(f"Error occurred for {club_name if 'club_name' in locals() else url}: {str(e)}")
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
            start_hour, start_minute = map(int, slot['start_time'].split(':'))
            end_hour, end_minute = map(int, slot['end_time'].split(':'))

            duration_minutes = (end_hour - start_hour) * 60 + (end_minute - start_minute)

            slot_data = {
                'court_id': court_id,
                'booking_date': booking_date,
                'start_time': slot['start_time'],
                'end_time': slot['end_time'],
                'availability': False,
                'duration_minutes': duration_minutes,
                'scrape_id': scrape_id,
                'scrape_timestamp': structured_data["scrape_timestamp"]
            }
            slots_data.append(slot_data)
    return slots_data


def save_json(structured_data):
    try:
        base_folder = 'webscrape/Scraped data/supabase_ready'
        today_folder = os.path.join(base_folder, datetime.now().strftime('%Y-%m-%d'))
        os.makedirs(today_folder, exist_ok=True)

        combined_file_path = os.path.join(today_folder, 'courts.json')

        if os.path.exists(combined_file_path):
            with open(combined_file_path, 'r') as f:
                existing_data = json.load(f)
        else:
            existing_data = {
                "booking_date": structured_data['booking_date'],
                "scrape_timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "clubs": []
            }

        club_entry = {
            "club_name": structured_data['club_name'],
            "courts": structured_data['courts']
        }

        existing_data['clubs'].append(club_entry)

        with open(combined_file_path, 'w') as f:
            json.dump(existing_data, f, indent=4)

        print(f"Saved club '{structured_data['club_name']}' to {combined_file_path}")

    except Exception as e:
        print(f"Error saving combined JSON: {str(e)}")


def insert_into_supabase(slots_data):
    try:
        if not slots_data:
            return

        batch_size = 100
        total_slots = len(slots_data)
        successful_inserts = 0
        errors = 0

        print(f"Inserting {total_slots} slots into Supabase... (historical mode)")

        for i in range(0, total_slots, batch_size):
            batch = slots_data[i:i + batch_size]
            try:
                response = supabase.table("slots").insert(batch).execute()
                if response.data:
                    successful_inserts += len(response.data)
            except Exception as e:
                errors += len(batch)
                print(f"Error inserting batch: {str(e)}")

        print(f"✔️ {successful_inserts} slots inserted. ❌ {errors} slots failed.")

    except Exception as e:
        print(f"Fatal error inserting into Supabase: {str(e)}")


def ensure_court_exists(club_id, court_name):
    try:
        if not club_id:
            return None

        response = supabase.table("courts").select("*").eq("club_id", club_id).eq("name", court_name).execute()

        if response.data and len(response.data) > 0:
            return response.data[0]['id']
        else:
            court_data = {
                'club_id': club_id,
                'name': court_name,
                'created_at': datetime.now().isoformat()
            }
            create_response = supabase.table("courts").insert(court_data).execute()
            if create_response.data and len(create_response.data) > 0:
                return create_response.data[0]['id']
    except Exception as e:
        print(f"Error ensuring court exists: {str(e)}")
    return None


def create_scrape_run():
    try:
        scrape_run_data = {
            'run_at': datetime.now().isoformat(),
            'booking_date': datetime.now().strftime('%Y-%m-%d'),
            'source': 'padel_scrape_enhanced.py',
            'notes': 'Automated scrape of playtomic courts',
            'slots_scraped': 0,
            'clubs_covered': 0,
            'scrape_status': 'in_progress'
        }
        response = supabase.table("scrape_runs").insert(scrape_run_data).execute()
        return response.data[0]['id'] if response.data else None
    except Exception as e:
        print(f"Error creating scrape run: {str(e)}")
        return None


def update_scrape_run(scrape_id, status, slots_scraped, clubs_covered):
    try:
        update_data = {
            'scrape_status': status,
            'slots_scraped': slots_scraped,
            'clubs_covered': clubs_covered
        }
        response = supabase.table("scrape_runs").update(update_data).eq('id', scrape_id).execute()
        return True
    except Exception as e:
        print(f"Error updating scrape run: {str(e)}")
        return False


def scrape_all_clubs(club_data, scrape_id):
    slots_per_club = {}
    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = {executor.submit(get_court_availability, club['url'], club_data, scrape_id): club['name'] for club in club_data}

        for future in futures:
            club_name = futures[future]
            try:
                slot_count = future.result()
                slots_per_club[club_name] = slot_count
            except Exception as e:
                print(f"Error processing {club_name}: {str(e)}")
                slots_per_club[club_name] = 0
    return slots_per_club


def main():
    scrape_id = create_scrape_run()
    if not scrape_id:
        print("Failed to create scrape run. Exiting.")
        return

    club_data = fetch_club_urls()
    print(f"Fetched {len(club_data)} clubs from Supabase")

    print(f"Processing all {len(club_data)} clubs")

    slots_per_club = scrape_all_clubs(club_data, scrape_id)
    total_slots = sum(slots_per_club.values())

    update_scrape_run(scrape_id, 'completed', total_slots, len(club_data))
    print(f"Scrape run {scrape_id} completed with {total_slots} slots and {len(club_data)} clubs.")
    print("Scraping process completed successfully!")


if __name__ == "__main__":
    main()
