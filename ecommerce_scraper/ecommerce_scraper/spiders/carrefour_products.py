import scrapy
from ..items import ProductItem
from scrapy_playwright.page import PageMethod


class CarrefourSpider(scrapy.Spider):
    name = "carrefour"
    allowed_domains = ["carrefour.ke"]
    start_urls = ["https://www.carrefour.ke/sitemap.xml"]

    # Default delays
    sitemap_delay = 0.5
    product_delay = 2
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen_urls = set()
        
    def parse(self, response):
        """Parse the main sitemap index and follow product sitemaps."""
        product_sitemaps = response.xpath(
            "//*[local-name()='sitemap']/*[local-name()='loc'][contains(text(), 'products')]/text()"
        ).getall()

        if not product_sitemaps:
            self.logger.warning("⚠️ No product sitemaps found at %s", response.url)

        for sitemap_url in product_sitemaps:
            self.logger.info("📂 Following product sitemap: %s", sitemap_url)
            yield scrapy.Request(
                url=sitemap_url,
                callback=self.parse_sitemap,
                meta={
                    "download_delay": self.sitemap_delay,
                    "playwright": True,  # enable playwright
                },
                errback=self.handle_failure,
            )

    def parse_sitemap(self, response):
        """Extract product URLs from each sitemap file."""
        product_urls = response.xpath("//url/loc/text()").getall()

        if not product_urls:
            self.logger.warning("⚠️ No product URLs found in sitemap: %s", response.url)

        for loc in product_urls:
            
            if loc not in self.seen_urls:
                self.seen_urls.add(loc)
                yield scrapy.Request(
                    url=loc,
                    callback=self.parse_product,
                    meta={
                        "download_delay": self.product_delay,
                        "playwright": True,
                        "playwright_page_methods": [
                            PageMethod("wait_for_load_state", "domcontentloaded"),
                        ],
                    },
                    errback=self.handle_failure,
                )

    def parse_product(self, response):
        """Extract fields from product page safely."""
        item = ProductItem()

        item["title"] = response.css("h1.css-106scfp::text").get()
        item["size"] = response.css("div.css-1kxxv3q::text").get()
        item["brand_name"] = response.css("a.css-1nnke3o::text").get()

        brand_link = response.css("a.css-1nnke3o::attr(href)").get()
        item["brand_link"] = response.urljoin(brand_link) if brand_link else None

        item["current_price"] = response.css("h2.css-1i90gmp::text").get()
        item["old_price"] = response.css("h2.css-1i90gmp del::text").get()
        item["discount_percent"] = response.css("span.css-2lm0bk::text").get()
        item["remaining_stock"] = response.css("div.css-g4iap9::text").get()
        item["product_highlight"] = response.css("div.css-1npift7::text").get()
        item["product_image"] = response.css("div.css-1d0skzn img::attr(data-src)").get()
        item["product_description"] = response.css("div.css-1weog53::text").get()
        item["url"] = response.url

        yield item

    def handle_failure(self, failure):
        """Optional: logs failed requests"""
        self.logger.error(
            "❌ Request failed: %s (reason: %s)", failure.request.url, getattr(failure, "value", None)
        )
