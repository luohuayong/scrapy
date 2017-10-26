#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by Leo on 2017/8/23

"""
代码说明：
scrapy 官方教程示例3
项目目录下运行命令
scrapy crawl quotes1
爬取数据并获取匹配内容字段
"""
import scrapy


class QuotesSpider1(scrapy.Spider):
    name = "quotes1"

    def start_requests(self):
        urls = [
            'http://quotes.toscrape.com/page/1/',
            'http://quotes.toscrape.com/page/2/',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        for quote in response.xpath("//div[@class='quote']"):
            yield {
                'text': quote.xpath("//span/text()").extract_first(),
                'author': quote.xpath("//small[@class='author']/text()").extract_first(),
                'tags': quote.xpath("//div[@class='tags']/a[@class='tag']/text()").extract(),
            }



if __name__ == '__main__':
    pass