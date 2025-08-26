from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time

def get_product_urls():
    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")

    driver = webdriver.Chrome(options=chrome_options)

    product_urls = []

    try:
        # Step 1: open main sitemap
        sitemap_url = "https://www.carrefour.ke/sitemap.xml"
        driver.get(sitemap_url)
        time.sleep(5)

        soup = BeautifulSoup(driver.page_source, "html.parser")

        # Step 2: find product sitemap URLs
        product_sitemap_links = [
            span.get_text(strip=True)
            for span in soup.find_all("span")
            if span.get_text(strip=True).startswith("https://www.carrefour.ke/sitemaps/products")
        ]

        print(f"Found {len(product_sitemap_links)} product sitemap files.")

        # Step 3: loop through each product sitemap
        for sm_link in product_sitemap_links:
            print(f"Visiting: {sm_link}")
            driver.get(sm_link)
            time.sleep(3)

            sm_soup = BeautifulSoup(driver.page_source, "html.parser")

            # Extract product URLs containing "/p/"
            for span in sm_soup.find_all("span"):
                text = span.get_text(strip=True)
                if "/p/" in text:
                    product_urls.append(text)

        print(f"\nExtracted {len(product_urls)} product URLs.")
        for url in product_urls[:20]:  # show first 20 for preview
            print(url)

    finally:
        driver.quit()

    return product_urls


if __name__ == "__main__":
    urls = get_product_urls()
