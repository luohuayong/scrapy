# -*- coding: utf-8 -*-
import scrapy
import json
from scrapy import Request
from scrapy.selector import Selector

class LianjiaXiaoquSpider(scrapy.Spider):
    name = 'lianjia_xiaoqu'
    allowed_domains = ['lianjia.com']
    start_urls = ['https://wh.lianjia.com/xiaoqu/']

    def start_requests(self):
        for url in self.start_urls:
            yield Request(url=url, callback=self.parse_index)
            # for i in range(100):
            #     url = self.base_url.format(i)
            #     yield Request(url=url, callback=self.parse_houseList)

    def parse_index(self,response):
        self.logger.debug("解析小区首页")
        quList = response.xpath("//div[@data-role='ershoufang']//a").extract()
        for i in range(1,len(quList)):
            item = quList[i]
            qu_href = Selector(text=item).xpath("//a/@href").extract_first()
            qu_name = Selector(text=item).xpath("//a/text()").extract_first()
            url = response.urljoin(qu_href)
            yield Request(url=url,callback=self.parse_qu,meta={'qu_name':qu_name})

    def parse_qu(self,response):
        self.logger.debug("解析区")
        qu_name = response.meta['qu_name']
        pianList = response.xpath("//div[@data-role='ershoufang']/div[2]/a").extract()
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
        xiaoquList = response.xpath("//div[@class='title']/a/text()").extract()
        for xiaoqu_name in xiaoquList:
            yield {
                'city_name':'武汉',
                'qu_name':qu_name,
                'pian_name':pian_name,
                'xiaoqu_name':xiaoqu_name,
            }
        str_page = response.xpath("//div[@class='page-box house-lst-page-box']/@page-data").extract_first()
        if(str_page and str_page != ''):
            json_page = json.loads(str_page)
            totalPage = json_page['totalPage']
            for i in range(2,totalPage+1):
                url = response.urljoin('/xiaoqu/baibuting/pg{}/'.format(i))
                yield Request(url=url,callback=self.parse_list,
                              meta={'qu_name':qu_name,'pian_name':pian_name})

    def parse(self, response):
        pass
