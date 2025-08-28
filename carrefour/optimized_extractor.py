import os
import json
import random
import asyncio
from csv import DictWriter
from dotenv import load_dotenv
import pandas as pd
from playwright.async_api import (
    async_playwright,
    TimeoutError as PlaywrightTimeoutError,
)

# --- Load Environment Variables ---
load_dotenv() 

# --- Configuration ---
MAX_CONCURRENT_TASKS = 3
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
STEALTH_JS_PATH = "stealth.min.js"


# --- Helper Functions ---
def get_nested_value(data, keys, default=None):
    """Safely access nested dictionary keys."""
    for key in keys:
        if isinstance(data, dict):
            data = data.get(key)
        elif isinstance(data, list) and len(data) > 0:
            data = data[0]
        else:
            return default
    return data


async def human_like_delay(min_sec=0.5, max_sec=1.5):
    """Wait for a random duration to mimic human behavior."""
    await asyncio.sleep(random.uniform(min_sec, max_sec))


# --- Main Scraping Logic (Now with Retries) ---
async def scrape_product(page, url, retries=3):
    """Scrapes a single product URL, with retries for timeouts."""
    for attempt in range(retries):
        try:
            print(f"Navigating to {url} (Attempt {attempt + 1}/{retries})...")
            # Increased timeout and changed wait_until state for more reliability
            await page.goto(url, wait_until="networkidle", timeout=60000)

            await page.mouse.move(random.randint(100, 800), random.randint(100, 800))
            await human_like_delay()
            await page.evaluate(
                "window.scrollBy(0, {})".format(random.randint(100, 400))
            )

            next_data_locator = page.locator("script#__NEXT_DATA__")
            content = await next_data_locator.text_content(timeout=15000)
            if not content:
                print(f"ERROR: __NEXT_DATA__ not found on {url}")
                continue  # Go to the next retry attempt

            data = json.loads(content)
            product_data = get_nested_value(
                data, ["props", "pageProps", "product"]
            ) or get_nested_value(
                data,
                ["props", "initialProps", "pageProps", "initialData", "products", 0],
            )

            if not product_data:
                print(
                    f"ERROR: Product data structure not found in __NEXT_DATA__ on {url}"
                )
                return None  # If structure is missing, retrying won't help

            product_info = {
                "id": product_data.get("id"),
                "ean": get_nested_value(product_data, ["attributes", "ean"]),
                "sku": get_nested_value(
                    product_data, ["offers", 0, "stores", 0, "storeData", "sku"]
                ),
                "title": product_data.get("title"),
                "brandName": get_nested_value(
                    product_data, ["attributes", "brandName"]
                ),
                "brandCode": get_nested_value(
                    product_data, ["attributes", "brandCode"]
                ),
                "description": get_nested_value(
                    product_data, ["attributes", "description"]
                ),
                "price": get_nested_value(
                    product_data, ["offers", 0, "stores", 0, "price", "value"]
                ),
                "currency": get_nested_value(
                    product_data, ["offers", 0, "stores", 0, "price", "currencyISO"]
                ),
                "stockStatus": get_nested_value(
                    product_data,
                    ["offers", 0, "stores", 0, "quantity", "stockIndicator", "status"],
                ),
                "url": url,
            }
            print(f"SUCCESS: Scraped {product_info['title']}")
            return product_info  # Success, exit the retry loop

        except PlaywrightTimeoutError:
            print(f"WARN: Timeout on attempt {attempt + 1}/{retries} for {url}")
            if attempt == retries - 1:
                print(f"ERROR: Final timeout after {retries} attempts for {url}")
                return None
            await human_like_delay(3, 5)  # Wait longer before retrying
        except Exception as e:
            print(f"ERROR: An unexpected error occurred for {url}: {e}")
            return None  # Non-timeout error, stop retrying for this URL
    return None


async def main(urls):
    """Main function to orchestrate the scraping of multiple URLs."""
    all_product_data = []
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

    async with async_playwright() as p:
        proxy_server = os.getenv("PROXY_SERVER")
        proxy_username = os.getenv("PROXY_USERNAME")
        proxy_password = os.getenv("PROXY_PASSWORD")
        
        browser = await p.chromium.launch(
            headless=False,
            proxy={
                "server": proxy_server,
                "username": proxy_username,
                "password": proxy_password,
            },
            args=["--disable-blink-features=AutomationControlled"],
        )

        context = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1920, "height": 1080},
            java_script_enabled=True,
            ignore_https_errors=True,
        )

        try:
            with open(STEALTH_JS_PATH, "r") as f:
                await context.add_init_script(f.read())
        except FileNotFoundError:
            print(f"WARNING: Stealth JS file not found at '{STEALTH_JS_PATH}'.")

        async def task(url):
            async with semaphore:
                page = await context.new_page()
                # --- DIAGNOSTIC STEP: Try commenting this out ---
                # If timeouts persist, Akamai might be detecting resource blocking.
                # await page.route("**/*", lambda route: route.abort() if route.request.resource_type in ["image", "font"] else route.continue_())

                result = await scrape_product(
                    page, url
                )  # The scrape_product function now handles its own retries
                if result:
                    all_product_data.append(result)
                await page.close()

        await asyncio.gather(*(task(url) for url in urls))
        await browser.close()

    return all_product_data


# --- Example Usage ---
if __name__ == "__main__":
    try:
        product_urls = pd.read_csv("carrefour_products.csv")["url"].sample(5).tolist()
    except FileNotFoundError:
        print("ERROR: carrefour_products.csv not found. Please ensure the file exists.")
        product_urls = []

    if product_urls:
        results = asyncio.run(main(product_urls))

        print("\n--- SCRAPING COMPLETE ---\n")

        if results:
            print(f"Successfully scraped {len(results)} products.")
            # Save to CSV safely
            try:
                with open(
                    "scraped_products.csv", "a+", encoding="utf-8", newline=""
                ) as f:
                    writer = DictWriter(f, fieldnames=results[0].keys())
                    #writer.writeheader()
                    writer.writerows(results)
                print("Data written to scraped_products.csv")
            except Exception as e:
                print(f"ERROR: Could not write to CSV file: {e}")
        else:
            print("No products were successfully scraped.")
