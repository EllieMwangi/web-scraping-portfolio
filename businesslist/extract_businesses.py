import os
import csv
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from tqdm import tqdm
import time

# --- Constants ---
BASE_URL = "https://www.businesslist.co.ke"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "..", "..", "data")
LOG_DIR = os.path.join(SCRIPT_DIR, "..", "..", "logs")
INPUT_CSV = os.path.join(DATA_DIR, "businesslist_categories.csv")
OUTPUT_CSV = os.path.join(DATA_DIR, "businesslist_listings.csv")
FAILED_LOG = os.path.join(LOG_DIR, "failed_categories.txt")

MAX_WORKERS = 6
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/115.0.0.0 Safari/537.36"
    )
}
LOCK = threading.Lock()


# --- Core Functions ---
def fetch_page(url):
    """Fetch page content with headers."""
    response = requests.get(url, headers=HEADERS, timeout=10)
    response.raise_for_status()
    return response.text


def extract_listings_from_page(html, category):
    """Extract business name and URL from a category page."""
    soup = BeautifulSoup(html, "html.parser")
    listings = []

    for div in soup.find_all("div", class_="company_header"):
        a_tag = div.find("h3").find("a")
        if not a_tag or not a_tag.get("href"):
            continue

        name = a_tag.text.strip()
        url = BASE_URL + a_tag["href"]
        listings.append((name, url, category))

    return listings


def find_next_page_url(html):
    """Get next page URL if exists, else None."""
    soup = BeautifulSoup(html, "html.parser")
    next_link = soup.find("a", class_="pages_arrow", rel="next")
    return BASE_URL + next_link["href"] if next_link and next_link.get("href") else None


def read_categories(csv_file):
    """Read categories from CSV and return list of (url, category_name)."""
    with open(csv_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [(row["url"], row["category"]) for row in reader]


def save_listings_incrementally(listings, filename):
    """Append rows to CSV (with header if new)."""
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    file_exists = os.path.exists(filename)

    with open(filename, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["company_name", "company_url", "category"])
        writer.writerows(listings)


def log_failed_category(category_name, category_url, error_msg):
    """Log failed categories to a separate file."""
    with LOCK:
        with open(FAILED_LOG, mode="a", encoding="utf-8") as f:
            f.write(f"{category_name},{category_url} -- {error_msg}\n")


# --- Main Worker ---
def scrape_category(category_url, category_name):
    current_url = category_url
    total = 0

    while current_url:
        try:
            print(f"  Scraping: {current_url}")
            html = fetch_page(current_url)
            listings = extract_listings_from_page(html, category_name)
            total += len(listings)

            if listings:
                with LOCK:
                    save_listings_incrementally(listings, OUTPUT_CSV)

            current_url = find_next_page_url(html)
            time.sleep(0.5)  # optional polite delay

        except requests.RequestException as e:
            log_failed_category(category_name, current_url, str(e))
            print(f"  ❌ Failed: {current_url} -- {e}")
            break

    return total


# --- Orchestrator ---
def scrape_all_businesses():
    categories = read_categories(INPUT_CSV)
    print(f"\n📦 Found {len(categories)} categories. Starting scraping with {MAX_WORKERS} threads...\n")

    total_listings = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(scrape_category, url, name): name
            for url, name in categories
        }

        for future in tqdm(as_completed(futures), total=len(futures), desc="🔄 Progress", unit="cat"):
            category_name = futures[future]
            try:
                result = future.result()
                total_listings += result
            except Exception as e:
                print(f"  ❌ Unexpected error in {category_name}: {e}")
                log_failed_category(category_name, "unknown", str(e))

    print(f"\n✅ Done! Total listings scraped: {total_listings}")
    print(f"📄 Results saved to: {OUTPUT_CSV}")
    if os.path.exists(FAILED_LOG):
        print(f"⚠️ Some categories failed. See log: {FAILED_LOG}")


if __name__ == "__main__":
    scrape_all_businesses()
