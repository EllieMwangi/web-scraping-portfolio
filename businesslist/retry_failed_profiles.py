import os
import csv
import json
import requests
import threading
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# --- Paths ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "..", "..", "data")
LOG_DIR = os.path.join(SCRIPT_DIR, "..", "..", "logs")
INPUT_CSV = os.path.join(DATA_DIR, "businesslist_listings.csv")
CSV_OUT = os.path.join(DATA_DIR, "businesslist_profiles.csv")
JSONL_OUT = os.path.join(DATA_DIR, "businesslist_profiles.jsonl")
FAILED_INPUT = os.path.join(LOG_DIR, "retry_failed_businesses.txt")
FAILED_RETRY_OUTPUT = os.path.join(LOG_DIR, "final_failed_businesses.txt")


# --- Constants ---
BASE_URL = "https://www.businesslist.co.ke"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/115.0.0.0 Safari/537.36"
    )
}
MAX_WORKERS = 6
LOCK = threading.Lock()

# Reuse the same helper functions from enrich_business_profiles.py:
def fetch_html(url):
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    return resp.text


def parse_business_profile(html, company_url):
    soup = BeautifulSoup(html, "html.parser")

    def text_or_none(selector, attr="text"):
        tag = soup.select_one(selector)
        if tag:
            return tag.text.strip() if attr == "text" else tag.get(attr)
        return None

    def extract_rating():
        rating_tag = soup.find("span", class_="rate")
        if rating_tag:
            for c in rating_tag.get("class", []):
                if c.startswith("rate_"):
                    return int(c.split("_")[-1])
        return None

    def extract_photos():
        photo_divs = soup.find_all("div", class_="photo_href", title="Company Photo")
        return [
            urljoin(BASE_URL, img["src"])
            for div in photo_divs
            for img in div.find_all("img")
            if img.get("src")
        ]

    def extract_phone_numbers():
        return [
            a.get("href").replace("tel:", "")
            for a in soup.find_all("a", href=True)
            if a["href"].startswith("tel:")
        ]

    def extract_operating_hours():
        hours_div = soup.find("div", id="open_hours")
        if not hours_div:
            return None
        result = {}
        for li in hours_div.find_all("li"):
            day = li.find("small")
            if day:
                day_name = day.text.strip(": ")
                time_range = li.get_text(strip=True).replace(day.text, "").strip()
                result[day_name] = time_range
        return result or None

    def extract_extra_info():
        extra = {}
        for info in soup.select("div.extra_info div.info"):
            label = info.find("div", class_="label")
            value = label.find_next_sibling(text=True)
            if label and value:
                extra[label.text.strip().lower().replace(" ", "_")] = value.strip()
        return extra or None

    def extract_description():
        desc_div = soup.find("div", class_="text desc")
        if not desc_div:
            return None
        table = desc_div.find("table")
        if table:
            desc = {}
            for row in table.find_all("tr"):
                cells = row.find_all(["th", "td"])
                if len(cells) == 2:
                    desc[cells[0].text.strip()] = cells[1].text.strip()
            return {"description": desc}
        return {"description": desc_div.get_text(strip=True)}

    def extract_tags():
        tag_div = soup.find("div", class_="tags")
        return [a.text.strip() for a in tag_div.find_all("a")] if tag_div else None

    return {
        "company_url": company_url,
        "tagline": text_or_none("div.tagline"),
        "rating": extract_rating(),
        "photo_links": extract_photos() or None,
        "company_name": text_or_none("div#company_name"),
        "address": text_or_none("div#company_address"),
        "maps_url": text_or_none("div.location_links a[rel='noopener']", "href"),
        "is_verified": bool(soup.find("i", attrs={"aria-label": "verified"})),
        "phone_numbers": extract_phone_numbers() or None,
        "website": text_or_none("div.text.weblinks a"),
        "operating_hours": extract_operating_hours(),
        "extra_information": extract_extra_info(),
        "company_description": extract_description(),
        "tags": extract_tags(),
    }


def save_to_csv(data, fieldnames):
    with LOCK:
        file_exists = os.path.exists(CSV_OUT)
        with open(CSV_OUT, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            row = {k: json.dumps(v) if isinstance(v, (dict, list)) else v for k, v in data.items()}
            writer.writerow(row)


def save_to_jsonl(data):
    with LOCK:
        with open(JSONL_OUT, "a", encoding="utf-8") as f:
            json.dump(data, f)
            f.write("\n")


def log_failed_retry(name, url, error):
    with LOCK:
        with open(FAILED_RETRY_OUTPUT, "a", encoding="utf-8") as f:
            f.write(f"{name},{url} -- {error}\n")


def process_retry(name, url, fieldnames):
    try:
        html = fetch_html(url)
        data = parse_business_profile(html, url)
        data.update({"company_name": name, "category": None})

        save_to_csv(data, fieldnames)
        save_to_jsonl(data)
        return True
    except Exception as e:
        log_failed_retry(name, url, str(e))
        return False


def main():
    if not os.path.exists(FAILED_INPUT):
        print("❌ No failed_businesses.txt file found.")
        return

    with open(FAILED_INPUT, encoding="utf-8") as f:
        lines = [
            line.strip().split(" -- ")[0]
            for line in f.readlines()
            if "--" in line
        ]

    fieldnames = [
        "company_name", "company_url", "category", "tagline", "rating",
        "photo_links", "address", "maps_url", "is_verified", "phone_numbers",
        "website", "operating_hours", "extra_information", "company_description", "tags"
    ]

    parsed_entries = []
    for entry in lines:
        try:
            name, url = entry.strip().split(",", 1)
            parsed_entries.append((name.strip(), url.strip()))
        except Exception:
            continue

    print(f"🔁 Retrying {len(parsed_entries)} failed businesses...\n")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_retry, name, url, fieldnames) for name, url in parsed_entries]
        for _ in tqdm(as_completed(futures), total=len(futures), desc="🔄 Retrying", unit="company"):
            pass

    print("\n✅ Retry attempt complete.")
    if os.path.exists(FAILED_RETRY_OUTPUT):
        print(f"⚠️ Still-failing URLs logged to: {FAILED_RETRY_OUTPUT}")


if __name__ == "__main__":
    main()
