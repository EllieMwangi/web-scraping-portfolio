import scrapy

class ProductItem(scrapy.Item):
    title = scrapy.Field()
    size = scrapy.Field()
    brand_name = scrapy.Field()
    brand_link = scrapy.Field()
    current_price = scrapy.Field()
    old_price = scrapy.Field()
    discount_percent = scrapy.Field()
    remaining_stock = scrapy.Field()
    product_highlight = scrapy.Field()
    product_image = scrapy.Field()
    product_description = scrapy.Field()
    url = scrapy.Field()
