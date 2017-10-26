#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by Leo on 2017/8/23

"""
代码说明：
scrapy 官方教程示例2
创建项目
scrapy startproject tutorial
项目目录下运行命令
scrapy crawl quotes
爬取数据并保存为.html文件
"""
import scrapy


class QuotesSpider(scrapy.Spider):
    name = "quotes"

    def start_requests(self):
        urls = [
            'http://quotes.toscrape.com/page/1/',
            'http://quotes.toscrape.com/page/2/',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        page = response.url.split("/")[-2]
        filename = 'quotes-%s.html' % page
        with open(filename, 'wb') as f:
            f.write(response.body)
        self.log('Saved file %s' % filename)


if __name__ == '__main__':
    pass

