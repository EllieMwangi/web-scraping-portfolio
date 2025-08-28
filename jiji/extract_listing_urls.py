import requests
import json
import csv
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


def format_attrs(attrs):
    """
    Converts attrs list into dict with lowercased keys and underscores instead of spaces
    """
    formatted = {}
    for attr in attrs:
        key = attr.get("name", "").strip().lower().replace(" ", "_")
        value = attr.get("value")
        if key:
            formatted[key] = value
    return formatted


def extract_seller(s):
    return {
        "advert_id": s.get("advert_id"),
        "adverts_count": s.get("adverts_count"),
        "date_created": s.get("date_created"),
        "feedback_count": s.get("feedback_count"),
        "guid": s.get("guid"),
        "id": s.get("id"),
        "image_url": s.get("image_url"),
        "name": s.get("name"),
        "page_url": s.get("page_url"),
        "status": s.get("status"),
    }


def extract_advert(advert):
    ad = advert.get("advert", {})
    seller = advert.get("seller", {})
    return {
        "category_id": ad.get("category_id"),
        "category_slug": ad.get("category_slug"),
        "attrs": format_attrs(ad.get("attrs", [])),
        "count_views": ad.get("count_views"),
        "date_created": ad.get("date_created"),
        "date_modified": ad.get("date_modified"),
        "description": ad.get("description"),
        "fav_count": ad.get("fav_count"),
        "guid": ad.get("guid"),
        "id": ad.get("id"),
        "images": [img.get("url") for img in ad.get("images", []) if img.get("url")],
        "is_active": ad.get("is_active"),
        "is_closed": ad.get("is_closed"),
        "is_in_moderation": ad.get("is_in_moderation"),
        "price_value": ad.get("price", {}).get("value"),
        "price_period": ad.get("price", {}).get("period"),
        "region_name": ad.get("region_name"),
        "region_slug": ad.get("region_slug"),
        "region_text": ad.get("region_text"),
        "title": ad.get("title"),
        "seller": extract_seller(seller),
    }


def extract_listing_guid(session, slug, page=1):
    url = "https://jiji.co.ke/api_web/v1/listing"
    params = {"slug": slug, "page": page}
    try:
        r = session.get(url, params=params)
        r.raise_for_status()
        data = r.json()
        total_pages = data.get("adverts_list", {}).get("total_pages", 0)
        adverts = data.get("adverts_list", {}).get("adverts", [])
        listing_guids = [advert["guid"] for advert in adverts if "guid" in advert]
        return listing_guids, total_pages
    except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
        print(f"Error fetching page {page}: {e}")
        return [], 0


def extract_listing_details(session, guid, slug):
    url = f"https://jiji.co.ke/api_web/v1/item/{guid}"
    session.headers.update({"Referer": f"https://jiji.co.ke/{slug}/{guid}.html"})

    max_retries = 3
    backoff_factor = 0.5  # Wait 0.5s, 1s, 2s between retries

    for attempt in range(max_retries):
        try:
            r = session.get(url)
            r.raise_for_status()
            data = r.json()
            return extract_advert(data)
        except requests.exceptions.HTTPError as e:
            # Check for "Too Many Requests" status code
            if e.response.status_code == 429 and attempt < max_retries - 1:
                wait_time = backoff_factor * (2**attempt)
                print(
                    f"Rate limit hit for GUID {guid}. Retrying in {wait_time:.2f}s..."
                )
                time.sleep(wait_time)
            else:
                print(f"HTTP error for GUID {guid}: {e}")
                return None  # Give up on other HTTP errors or after max retries
        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
            print(f"Error fetching details for GUID {guid}: {e}")
            return None

    print(f"Failed to fetch GUID {guid} after {max_retries} retries.")
    return None


def main():
    SLUG = "houses-apartments-for-rent"
    JSONL_FILE = "listings.jsonl"
    CSV_FILE = "listings.csv"
    MAX_WORKERS = 10  # Number of concurrent threads

    all_guids = []

    headers = {"User-Agent": "Mozilla/5.0"}
    with requests.Session() as session:
        session.headers.update(headers)

        # Make the first request to get the total number of pages
        print("Fetching page 1 to get total pages...")
        first_page_guids, total_pages = extract_listing_guid(session, SLUG, page=1)

        if not total_pages:
            print("Could not determine total pages. Exiting.")
            return

        all_guids.extend(first_page_guids)
        print(f"Found {total_pages} total pages.")

        # Loop through the remaining pages to get all GUIDs
        for page_num in range(2, total_pages + 1):
            print(f"Fetching GUIDs from page {page_num}...")
            page_guids, _ = extract_listing_guid(session, SLUG, page=page_num)
            if page_guids:
                all_guids.extend(page_guids)

    print(f"\nFound {len(all_guids)} total listings. Now fetching details...")

    # Prepare files for writing
    with (
        open(JSONL_FILE, "w") as jsonl_file,
        open(CSV_FILE, "w", newline="", encoding="utf-8") as csv_file,
    ):
        csv_writer = None  # Initialize csv_writer

        # Use ThreadPoolExecutor to fetch details concurrently
        with (
            requests.Session() as detail_session,
            ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor,
        ):
            detail_session.headers.update(headers)

            # Submit all detail extraction tasks
            future_to_guid = {
                executor.submit(
                    extract_listing_details, detail_session, guid, SLUG
                ): guid
                for guid in all_guids
            }

            for i, future in enumerate(as_completed(future_to_guid)):
                details = future.result()
                if details:
                    print(f"Processed {i + 1}/{len(all_guids)}: GUID {details['guid']}")

                    # Write to JSONL file
                    jsonl_file.write(json.dumps(details) + "\n")

                    # --- Write to CSV File ---
                    # Serialize nested fields to JSON strings instead of flattening
                    csv_ready_details = details.copy()
                    csv_ready_details["attrs"] = json.dumps(
                        csv_ready_details.get("attrs", {})
                    )
                    csv_ready_details["seller"] = json.dumps(
                        csv_ready_details.get("seller", {})
                    )
                    csv_ready_details["images"] = json.dumps(
                        csv_ready_details.get("images", [])
                    )

                    # For the first successful record, create the DictWriter and write the header
                    if csv_writer is None:
                        fieldnames = sorted(
                            csv_ready_details.keys()
                        )  # Sort keys for consistent column order
                        csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                        csv_writer.writeheader()

                    # Ensure all rows have the same headers and write the row
                    row_data = {
                        field: csv_ready_details.get(field) for field in fieldnames
                    }
                    csv_writer.writerow(row_data)

    print(f"\nScraping complete. Data saved to {JSONL_FILE} and {CSV_FILE}")


if __name__ == "__main__":
    main()
