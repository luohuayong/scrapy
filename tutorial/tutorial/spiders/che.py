# -*- coding: utf-8 -*-
import scrapy


class CheSpider(scrapy.Spider):
    name = 'che'
    allowed_domains = ['www.guazi.com']
    start_urls = ['https://www.guazi.com/wh/buy/',]

    def parse(self, response):
        for a in response.xpath("//span[@class='x-box']/a"):
            print a.extract()

