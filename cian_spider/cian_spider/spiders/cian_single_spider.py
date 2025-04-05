import scrapy
from ..items import CianItem

class CianSingleSpider(scrapy.Spider):
    name = "cian_single"
    allowed_domains = ["cian.ru"]

    def start_requests(self):
        # Замените URL на нужный
        yield scrapy.Request(url="https://www.cian.ru/sale/flat/301932383/", callback=self.parse)

    def parse(self, response):
        item = CianItem()
        item["url"] = response.url
        item["title"] = response.css('h1[data-mark="title"]::text').get()
        item["address"] = response.css('span[data-mark="address"]::text').get()
        item["price"] = response.css('span[data-mark="MainPrice"]::text').re_first(r'\d+[\s\d]*')
        params = response.css("li.a10a3f92e9--item--_ipjW::text").getall()
        item["area"] = next((p for p in params if "м²" in p), None)
        item["year"] = next((p for p in params if "год" in p.lower()), None)
        item["balcony"] = next((p for p in params if "балкон" in p.lower()), None)
        item["district"] = response.css('span[data-mark="district"]::text').get()
        yield item
