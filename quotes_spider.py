#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by Leo on 2017/8/23

"""
代码说明：
scrapy 官方教程示例1
单一文件爬虫
运行命令 scrapy runspider quotes_spider.py -o quotes.json
爬取数据并存储到json文件中
"""

import scrapy

class QuotesSpider(scrapy.Spider):
    name = "quotes"
    start_urls = ['http://quotes.toscrape.com/tag/humor/',]

    def parse(self, response):
        for quote in response.css('div.quote'):
            yield {
                'text': quote.css('span.text::text').extract_first(),
                'author': quote.xpath('span/small/text()').extract_first(),
            }

        next_page = response.css('li.next a::attr("href")').extract_first()
        if next_page is not None:
            yield response.follow(next_page, self.parse)

if __name__ == '__main__':
    pass