# -*- coding: utf-8 -*-
import logging
import scrapy
from scrapy import Request


class LianjiaSpider(scrapy.Spider):
    name = 'lianjia'
    allowed_domains = ['lianjia.com']
    start_urls = ['https://wh.lianjia.com/ershoufang/']
    logger = logging.getLogger(name=__name__)

    def start_requests(self):
        for url in self.start_urls:
            yield Request(url=url, callback=self.parse_First)

    def parse_First(self, response):
        self.logger.debug("解析首页")
        quList = response.xpath("//div[@data-role='ershoufang']//a/@href").extract()
        for i in range(1,100):
            for item in quList:
                base_url = item+'pg{}/'.format(i)
                url = response.urljoin(base_url)
                yield  Request(url=url, callback=self.parse_houseList)

    def parse_houseList(self, response):
        self.logger.debug("解析房屋列表")
        houseList = response.xpath("//div[@class='info clear']//div[@class='title']//a/@href").extract()
        for url in houseList:
            yield  Request(url=url, callback=self.parse_houseInfo)

    def str_space(self,str):
        result = ''
        if(str):
            result = str.replace(' ','').strip()
        return result

    def parse_houseInfo(self, response):
        self.logger.debug("解析房屋详情")

        danjia = response.xpath("//span[@class='unitPriceValue']/text()").extract_first()
        # huxing = response.xpath("//div[@class='room']/div[@class='mainInfo']/text()").\
        #     extract_first()
        niandai = response.xpath("//div[@class='area']/div[@class='subInfo']/text()"). \
            extract_first().split('/')[0]
        louceng = response.xpath("//div[@class='room']/div[@class='subInfo']/text()"). \
            extract_first().split('/')[0]
        zonglouceng = response.xpath("//div[@class='room']/div[@class='subInfo']/text()"). \
            extract_first().split('/')[1]

        xiaoqu = response.xpath("//div[@class='communityName']/a/text()").extract_first()
        qu = response.xpath("//div[@class='areaName']/span[@class='info']/a/text()").extract()[0]
        if(qu[len(qu)-1])!='区':
            qu = qu + '区'
        pian = response.xpath("//div[@class='areaName']/span[@class='info']/a/text()").extract()[1]

        huxing = response.xpath("//span[text()='房屋户型']/parent::*/text()").extract_first()
        huxing = huxing.split('厅')[0] + '厅' + huxing.split('厨')[1]
        mianji = response.xpath("//span[text()='建筑面积']/parent::*/text()").extract_first()
        chaoxiang = response.xpath("//span[text()='房屋朝向']/parent::*/text()").extract_first()
        zhuangxiu = response.xpath("//span[text()='装修情况']/parent::*/text()").extract_first()
        dianti = response.xpath("//span[text()='配备电梯']/parent::*/text()").extract_first()
        jianzhujiegou = response.xpath("//span[text()='户型结构']/parent::*/text()").extract_first()
        jianzhu = response.xpath("//span[text()='建筑类型']/parent::*/text()").extract_first()
        if(not jianzhu):
            jianzhu = response.xpath("//span[text()='别墅类型']/parent::*/text()").extract_first()
        nianxian = response.xpath("//span[text()='产权年限']/parent::*/text()").extract_first()
        guapai = response.xpath("//span[text()='挂牌时间']/parent::*/text()").extract_first()
        gengxin = guapai
        chanquan = response.xpath("//span[text()='交易权属']/parent::*/text()").extract_first()
        zhuzhaileibie = response.xpath("//span[text()='房屋用途']/parent::*/text()").extract_first()

        xuexiao = ""
        jianzhuleibie = ""
        jianzhuxingshi = ""
        junjia = ""
        leixing = ""
        wuyefei = ""
        lvhua = ""
        rongji = ""
        fenliu = ""
        loudong = ""
        hushu = ""

        yield {
            'chengshi': '武汉',
            'huxing': self.str_space(huxing),
            'mianji': self.str_space(mianji),
            'danjia': self.str_space(danjia),
            'chaoxiang': self.str_space(chaoxiang),
            'louceng': self.str_space(louceng),
            'zonglouceng': self.str_space(zonglouceng),
            'zhuangxiu': self.str_space(zhuangxiu),
            'xiaoqu': self.str_space(xiaoqu),
            'qu': self.str_space(qu),
            'pian': self.str_space(pian),
            'xuexiao': self.str_space(xuexiao),
            'niandai': self.str_space(niandai),
            'dianti': self.str_space(dianti),
            'chanquan': self.str_space(chanquan),
            'zhuzhaileibie': self.str_space(zhuzhaileibie),
            'jianzhujiegou': self.str_space(jianzhujiegou),
            'jianzhuleibie': self.str_space(jianzhuleibie),
            'jianzhuxingshi': self.str_space(jianzhuxingshi),
            'guapai': self.str_space(guapai),
            'gengxin': self.str_space(gengxin),
            'junjia': self.str_space(junjia),
            'leixing': self.str_space(leixing),
            'wuyefei': self.str_space(wuyefei),
            'jianzhu': self.str_space(jianzhu),
            'nianxian': self.str_space(nianxian),
            'lvhua': self.str_space(lvhua),
            'rongji': self.str_space(rongji),
            'fenliu': self.str_space(fenliu),
            'loudong': self.str_space(loudong),
            'hushu': self.str_space(hushu),
            'url': response.url,
            'laiyuan': 'lianjia.com'
        }

    def parse(self, response):
        pass
