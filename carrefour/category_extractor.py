import itertools
from csv import DictWriter
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import requests

def extract_headers():
    
# --- Start Selenium session ---
    chrome_options = Options()
    #chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")

    driver = webdriver.Chrome(options=chrome_options)

    # Visit Carrefour site
    driver.get("https://www.carrefour.ke/mafken/en/")

    # Grab cookies from Selenium
    selenium_cookies = driver.get_cookies()
    cookies_dict = {c['name']: c['value'] for c in selenium_cookies}

    # Grab User-Agent from Selenium
    user_agent = driver.execute_script("return navigator.userAgent;")

    driver.quit()
    
    return cookies_dict, user_agent

def extract_categories(headers, cookies_dict, user_agent):
    
     # Step 1: Fetch categories
    menu_url = "https://www.carrefour.ke/api/v1/menu"
    params = {
        "latitude": "-1.2672236834605626",
        "longitude": "36.810586556760555",
        "lang": "en",
        "displayCurr": "KES"
    }

    resp = requests.get(menu_url, headers=headers, cookies=cookies_dict, params=params)
    print("Menu status:", resp.status_code)

    category_list = []
    if resp.status_code == 200:
        data = resp.json()
        for item in data[0]['children']:
            category_list.append({'title': item.get('title'), 'id': item.get('id')})

    print("Extracted categories:", len(category_list))
    
    return category_list


def fetch_products_for_category(category_id, headers, cookies):
    all_products = []
    base_url = f"https://www.carrefour.ke/api/v8/categories/{category_id}"

    # First request to get pagination metadata
    params = {
        "filter": "",
        "sortBy": "relevance",
        "currentPage": 0,
        "pageSize": 100,
        "maxPrice": "",
        "minPrice": "",
        "areaCode": "Westlands - Nairobi",
        "lang": "en",
        "displayCurr": "KES",
        "latitude": "-1.2672236834605626",
        "longitude": "36.810586556760555",
        "needVariantsData": "true",
        "nextOffset": "",
        "requireSponsProducts": "true",
        "responseWithCatTree": "true",
        "depth": 3
    }

    resp = requests.get(base_url, headers=headers, cookies=cookies, params=params)
    data = resp.json()

    # Pagination metadata
    total_pages = data['pagination']['totalPages']
    total_results = data['pagination']['totalResults']
    print(f"Category {category_id} has {total_pages} pages and {total_results} products.")

    for page in range(0, total_pages + 1):
        params["currentPage"] = page
        resp = requests.get(base_url, headers=headers, cookies=cookies, params=params)
        data = resp.json()
        print(f"Page {page}: {len(data['products'])} products returned")


        for prod in data['products']:
            all_products.append({
            "name": prod.get("name"),
            "url": "https://www.carrefour.ke" + prod.get('links', {}).get('productUrl', {}).get('href', ""),
            "image": prod.get("links", {}).get("images", [{}])[0].get("href", ""),
            "category_hierachy": prod.get("productCategoriesHearchi","").split("/"),
            "price": prod.get("price", {}).get("formattedValue", ""),
            "supplier": prod.get("supplier", ""),
            "brand_name": prod.get("brand", {}).get("name", ""),
            "brand_id": prod.get("brand", {}).get("id", "")       
        })
        print(f"Fetched page {page}/{total_pages} ({len(all_products)} products so far)")


    print(f"Total products in category {category_id}: {len(all_products)}")    
    return all_products


if __name__ == "__main__":
    cookies_dict, user_agent = extract_headers()
    # --- Use cookies + UA in requests ---
    headers = {
        "accept": "application/json, text/plain, */*",
        "appid": "Reactweb",
        "storeid": "mafken",
        "userid": "anonymous",
        "x-maf-account": "carrefour",
        "x-maf-tenant": "mafretail",
        "x-requested-with": "XMLHttpRequest",
        "user-agent": user_agent,
        "referer": "https://www.carrefour.ke/mafken/en/"
    }
    categories = extract_categories(headers,cookies_dict, user_agent)
    
    product_details = []
    
    for cat in categories:
        print(f"Processing category: {cat['title']} (ID: {cat['id']})")
        products = fetch_products_for_category(cat['id'], headers, cookies_dict)
        product_details.extend(products)
    
    print(f"Total products extracted across all categories: {len(product_details)}")
    
    
    with open("carrefour_products.csv", "w", encoding="utf-8", newline='\n') as f:
        writer = DictWriter(f, fieldnames=product_details[0].keys())
        writer.writeheader()
        writer.writerows(product_details)
        print("Data written to carrefour_products.csv")