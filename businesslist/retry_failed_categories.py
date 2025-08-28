import os
import csv
import requests
from bs4 import BeautifulSoup

# Paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "..", "..", "data")
LOG_DIR = os.path.join(SCRIPT_DIR, "..", "..", "logs")
FAILED_FILE = os.path.join(LOG_DIR, "failed_categories.txt")
RETRY_FAILED_FILE = os.path.join(LOG_DIR, "retry_failed.txt")
OUTPUT_CSV = os.path.join(DATA_DIR, "businesslist_listings.csv")

BASE_URL = "https://www.businesslist.co.ke"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/115.0.0.0 Safari/537.36"
    )
}


def fetch_page(url):
    response = requests.get(url, headers=HEADERS, timeout=10)
    response.raise_for_status()
    return response.text


def extract_listings(html, category):
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


def save_listings(listings):
    file_exists = os.path.exists(OUTPUT_CSV)
    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["company_name", "company_url", "category"])
        writer.writerows(listings)


def retry_failed_pages():
    if not os.path.exists(FAILED_FILE):
        print("No failed_categories.txt file found.")
        return

    with open(FAILED_FILE, encoding="utf-8") as f:
        lines = f.readlines()

    print(f"🔁 Retrying {len(lines)} failed pages...\n")

    for line in lines:
        if "--" not in line:
            continue

        try:
            category_info, _ = line.strip().split("--", 1)
            category_name, category_url = category_info.strip().split(",", 1)
            print(f"🔎 Retrying: {category_name} | {category_url}")

            html = fetch_page(category_url.strip())
            listings = extract_listings(html, category_name.strip())

            if listings:
                save_listings(listings)
                print(f"✅ Success: {len(listings)} listings extracted\n")
            else:
                print(f"⚠️  No listings found on page: {category_url.strip()}")

        except Exception as e:
            print(f"❌ Retry failed for {line.strip()}: {e}")
            with open(RETRY_FAILED_FILE, "a", encoding="utf-8") as out:
                out.write(f"{line.strip()} -- {e}\n")


if __name__ == "__main__":
    retry_failed_pages()
