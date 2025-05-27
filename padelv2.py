import os
import time
import uuid
import logging
import argparse
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from supabase import create_client
from playwright.async_api import async_playwright
import asyncio

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
    supabase.table("scrape_runs").insert(payload).execute()
    logger.info("Created scrape run %s", scrape_id)
    return scrape_id

def fetch_clubs():
    res = supabase.table("clubs").select("id,name,url").execute()
    return res.data if res.data else []

# --- Scraper Function ---
async def scrape_club(playwright, club, scrape_id, booking_date):
    browser = await playwright.chromium.launch(headless=True)
    context = await browser.new_context()
    page = await context.new_page()

    try:
        logger.info("Scraping %s", club["url"])
        await page.goto(club["url"], timeout=60000)

        try:
            await page.click("button:has-text('Accept')", timeout=3000)
        except:
            pass

        try:
            await page.wait_for_selector("div.border-b", state="attached", timeout=60000)
        except Exception as e:
            logger.error("Playwright error for club %s: %s", club["name"], e)
            return []

        elements = await page.query_selector_all("div.border-b")
        logger.info("Found %d slots for club: %s", len(elements), club["name"])

        # Place slot parsing logic here if needed
        return []

    finally:
        await context.close()
        await browser.close()

# --- Main ---
async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Number of clubs to process")
    args = parser.parse_args()

    scrape_id = create_scrape_run()
    booking_date = datetime.now().date().isoformat()
    clubs = fetch_clubs()
    logger.info("Found %d clubs", len(clubs))

    to_process = clubs[:args.limit] if args.limit else clubs

    async with async_playwright() as playwright:
        for club in to_process:
            await scrape_club(playwright, club, scrape_id, booking_date)

if __name__ == "__main__":
    asyncio.run(main())
