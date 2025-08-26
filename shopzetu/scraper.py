import requests
import csv
import time
import json

BASE_URL = "https://shopzetu.com/api/collections/{handle}/products"
HEADERS = {"User-Agent": "Mozilla/5.0"}

def fetch_products(handle, cursor=None, first=24):
    url = BASE_URL.format(handle=handle)
    params = {"first": first}
    if cursor:
        params["cursor"] = cursor
    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json()

def scrape_collection(handle, first=24, delay=1):
    """Scrape all products from a single collection handle."""
    all_products = []
    cursor = None
    has_next = True

    while has_next:
        data = fetch_products(handle, cursor, first)

        products = data.get("products", [])
        for p in products:
            all_products.append({
                "collection_handle": handle,
                "title": p.get("title"),
                "id": p.get("id"),
                "createdAt": p.get("createdAt"),
                "url": f"https://shopzetu.com/products/{p.get('handle')}",
                "image_url": (
                    p.get("featuredImage", {}).get("url")
                    if p.get("featuredImage") else None
                ),
                "price_min": (
                    p.get("priceRange", {}).get("minVariantPrice", {}).get("amount")
                    if p.get("priceRange") else None
                ),
                "price_max": (
                    p.get("priceRange", {}).get("maxVariantPrice", {}).get("amount")
                    if p.get("priceRange") else None
                ),
                "variants": p.get("variants", {}).get("nodes", []),
                "vendor": p.get("vendor"),
                "tags": p.get("tags", [])
            })

        # Pagination
        page_info = data.get("pageInfo", {})
        has_next = page_info.get("hasNextPage", False)
        cursor = page_info.get("endCursor")

        print(f"[{handle}] Fetched {len(products)} products, total {len(all_products)}")
        time.sleep(delay)

    return all_products

def save_to_csv(products, filename="shopzetu_products.csv"):
    """Save product data to CSV. Variants & tags saved as JSON strings."""
    if not products:
        print("No products found.")
        return

    keys = [
        "collection_handle", "title", "id", "createdAt",
        "url", "image_url", "price_min", "price_max",
        "vendor", "tags", "variants"
    ]

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for p in products:
            row = p.copy()
            row["variants"] = json.dumps(row["variants"], ensure_ascii=False)
            row["tags"] = json.dumps(row["tags"], ensure_ascii=False)
            writer.writerow(row)

    print(f"✅ Saved {len(products)} products to {filename}")

if __name__ == "__main__":
    # 👇 Add any number of collection handles here
    collection_handles = ["new-arrivals","women","exclusively-men"]
    for handle in collection_handles:
        products = scrape_collection(handle, first=24, delay=1)
        save_to_csv(products, f"{handle}-products.csv")
