# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy_redis_loadbalancing.spiders import RedisSpider


class TestSpider(RedisSpider):
    name = 'test'
#    allowed_domains = ['localhost']
#    start_urls = ['http://localhost:8998/']
    link_extractor = LinkExtractor()

    def parse(self, response):
        for link in self.link_extractor.extract_links(response):
            yield scrapy.Request(link.url, callback=self.parse)
