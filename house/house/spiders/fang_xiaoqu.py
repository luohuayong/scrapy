# -*- coding: utf-8 -*-
import logging
import scrapy
from scrapy import Request
from scrapy.selector import Selector


class FangXiaoquSpider(scrapy.Spider):
    name = 'fang_xiaoqu'
    allowed_domains = ['fang.com']
    start_urls = ['http://esf.wuhan.fang.com/housing/']

    def start_requests(self):
        for url in self.start_urls:
            yield Request(url=url, callback=self.parse_index)
        # for i in range(100):
        #     url = self.base_url.format(i)
        #     yield Request(url=url, callback=self.parse_houseList)

    def parse_index(self,response):
        self.logger.debug("解析小区首页")
        quList = response.xpath("//div[@class='qxName']/a").extract()
        for i in range(1,len(quList)):
            item = quList[i]
            qu_href = Selector(text=item).xpath("//a/@href").extract_first()
            qu_name = Selector(text=item).xpath("//a/text()").extract_first()
            url = response.urljoin(qu_href)
            yield Request(url=url,callback=self.parse_qu,meta={'qu_name':qu_name})

    def parse_qu(self,response):
        self.logger.debug("解析区")
        qu_name = response.meta['qu_name']
        pianList = response.xpath("//p[@id='shangQuancontain']/a").extract()
        for i in range(1,len(pianList)):
            item = pianList[i]
            pian_href = Selector(text=item).xpath("//a/@href").extract_first()
            pian_name = Selector(text=item).xpath("//a/text()").extract_first()
            url = response.urljoin(pian_href)
            yield Request(url=url,callback=self.parse_list,
                          meta={'qu_name':qu_name,'pian_name':pian_name})

    def parse_list(self,response):
        self.logger.debug("解析小区列表")
        qu_name = response.meta['qu_name']
        pian_name = response.meta['pian_name']
        xiaoquList = response.xpath("//a[@class='plotTit']/text()").extract()
        for xiaoqu_name in xiaoquList:
            yield {
                'city_name':'武汉',
                'qu_name':qu_name,
                'pian_name':pian_name,
                'xiaoqu_name':xiaoqu_name,
            }
        next_page = response.xpath("//a[@id='PageControl1_hlk_next']/@href").extract_first()
        if(next_page):
            url = response.urljoin(next_page)
            yield Request(url=url,callback=self.parse_list,
                          meta={'qu_name':qu_name,'pian_name':pian_name})

    def parse(self, response):
        pass
