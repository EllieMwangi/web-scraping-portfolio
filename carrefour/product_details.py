import asyncio
import json
from playwright.async_api import async_playwright

async def scrape_product(url):
    async with async_playwright() as p:
        # Launch Chromium with anti-detection args
        browser = await p.chromium.launch(
            headless=False,  # Start visible; set True later
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage"
            ]
        )

        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/139.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
            java_script_enabled=True
        )

        # Optional: block unnecessary resources
        await context.route("**/*", lambda route: route.abort() 
                            if route.request.resource_type in ["image", "font", "stylesheet"] 
                            else route.continue_())

        page = await context.new_page()

        # Navigate slowly to avoid detection
        await page.goto(url, wait_until="domcontentloaded")
        await page.wait_for_timeout(1000)  # 1s delay

        # Extract __NEXT_DATA__ JSON
        content = await page.locator("script#__NEXT_DATA__").text_content()
        data = json.loads(content)

        # Navigate to product details
        product = data["props"]["initialProps"]["pageProps"]["initialData"]["products"][0]
        #print(json.dumps(product, indent=2))  # For debugging
        # Extract key info
        product_info = {
            # Identity
            "id": product.get("id"),
            "sku": product.get("offers", [{}])[0].get("stores", [{}])[0].get("storeData", {}).get("sku"),
            "ean": product.get("attributes", {}).get("ean"),
            "url": product.get("url"),

            # Basic info
            "title": product.get("title"),
            "description": product.get("attributes", {}).get("description"),
            "brandName": product.get("attributes", {}).get("brandName"),
            "brandCode": product.get("attributes", {}).get("brandCode"),

            # Size / measure
            "size": product.get("attributes", {}).get("size"),
            "soldByWeight": product.get("attributes", {}).get("soldByWeight"),

            # Categories (with level + name)
            "categories": [
                {"level": c.get("level"), "name": c.get("name")}
                for c in product.get("categories", [])
            ],
            "productType": product.get("attributes", {}).get("productType"),
            "nature": product.get("attributes", {}).get("nature"),

            # Pricing
            "pricing": {
                "currency": product.get("offers", [{}])[0].get("stores", [{}])[0].get("price", {}).get("currencyISO"),
                "price": product.get("offers", [{}])[0].get("stores", [{}])[0].get("price", {}).get("value"),
                "originalPrice": product.get("offers", [{}])[0].get("stores", [{}])[0].get("price", {}).get("original", {}).get("value"),
                "discountPrice": product.get("offers", [{}])[0].get("stores", [{}])[0].get("price", {}).get("discount", {}).get("value"),
                "discountPercent": product.get("offers", [{}])[0].get("stores", [{}])[0].get("price", {}).get("discount", {}).get("information", {}).get("amount"),
                "discountEndDate": product.get("offers", [{}])[0].get("stores", [{}])[0].get("price", {}).get("discount", {}).get("information", {}).get("discountEndDate"),
            },

            # Availability
            "availability": {
                "stockStatus": product.get("offers", [{}])[0].get("stores", [{}])[0].get("quantity", {}).get("stockIndicator", {}).get("status"),
                "stockLevel": product.get("offers", [{}])[0].get("stores", [{}])[0].get("quantity", {}).get("stockIndicator", {}).get("value"),
                "minToOrder": product.get("offers", [{}])[0].get("stores", [{}])[0].get("quantity", {}).get("minToOrder"),
                "maxToOrder": product.get("offers", [{}])[0].get("stores", [{}])[0].get("quantity", {}).get("maxToOrder"),
                "incrementBy": product.get("offers", [{}])[0].get("stores", [{}])[0].get("quantity", {}).get("increments"),
                "UnitType": product.get("offers", [{}])[0].get("stores", [{}])[0].get("quantity", {}).get("units"),
            },

            # Badges (only type)
            "badges":[val.get("type") for val in product.get("badges", {}).get("promo-badges", [{}])],
            # Highlight / marketing
            "highlight": product.get("attributes", {}).get("marketingText"),

            # SEO
            "seo": {
                "metaTitle": product.get("seoAttributes", {}).get("metaTitle"),
                "metaDescription": product.get("seoAttributes", {}).get("metaDescription"),
            },

            # Origin
            "origin_country": product.get("attributes", {}).get("countryOrigin"),

            # Media (only primary image)
            "media": product.get("gallery", [{}])[0].get("url")
        }


        await browser.close()
        return product_info

# Example usage
if __name__ == "__main__":
    url = "https://www.carrefour.ke/mafken/en/clementine-mandarin/tangerine-import/p/1328"
    info = asyncio.run(scrape_product(url))
    print(json.dumps(info, indent=2))
