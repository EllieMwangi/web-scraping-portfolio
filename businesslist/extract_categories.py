import requests
from bs4 import BeautifulSoup
import csv
import os

BASE_URL = "https://www.businesslist.co.ke"
TARGET_URL = f"{BASE_URL}/browse-business-directory"

# Resolve path to the data directory relative to the script location
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "..", "..", "data")
OUTPUT_FILE = os.path.join(DATA_DIR, "businesslist_categories.csv")


def fetch_page(url):
    
    """Fetch the content of the given URL with headers to avoid 403 errors."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/115.0.0.0 Safari/537.36"
        )
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.text


def parse_category_list(html):
    """Parse the HTML and extract category info from <ul class="icats">."""
    soup = BeautifulSoup(html, "html.parser")
    category_data = []

    ul_elements = soup.find_all("ul", class_="icats")

    for ul in ul_elements:
        li_elements = ul.find_all("li")

        for li in li_elements:
            a_tag = li.find("a")
            if not a_tag or not a_tag.get("href"):
                continue

            href = a_tag["href"]
            full_url = BASE_URL + href
            category_name = a_tag.contents[0].strip()

            span_tag = a_tag.find("span")
            business_count = 0
            if span_tag:
                business_count = int(span_tag.text.replace(",", "").strip())

            category_data.append((full_url, category_name, business_count))

    return category_data


def save_to_csv(data, filename):
    """Save extracted data to a CSV file."""
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["url", "category", "business_count"])
        writer.writerows(data)


def main():
    print("Fetching page content...")
    html = fetch_page(TARGET_URL)

    print("Parsing category data...")
    category_data = parse_category_list(html)

    print(f"Saving {len(category_data)} records to {OUTPUT_FILE}...")
    save_to_csv(category_data, OUTPUT_FILE)

    print("Done!")


if __name__ == "__main__":
    main()
