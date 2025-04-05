import scrapy

class CianItem(scrapy.Item):
    title = scrapy.Field()
    address = scrapy.Field()
    district = scrapy.Field()
    area = scrapy.Field()
    year = scrapy.Field()
    price = scrapy.Field()
    url = scrapy.Field()
    balcony = scrapy.Field()
