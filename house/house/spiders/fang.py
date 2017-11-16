# -*- coding: utf-8 -*-
import logging
import scrapy
from scrapy import Request


class FangSpider(scrapy.Spider):
    name = 'fang'
    allowed_domains = ['fang.com']
    # start_urls = ['http://esf.wuhan.fang.com/',
    #               'http://esf.wuhan.fang.com/house/i31/']
    base_url = 'http://esf.wuhan.fang.com/house/i3{0}/'
    logger = logging.getLogger(name=__name__)

    def start_requests(self):
        # for url in self.start_urls:
        #     yield Request(url=url, callback=self.parse_houseList)
        for i in range(100):
            url = self.base_url.format(i)
            yield Request(url=url, callback=self.parse_houseList)

    def parse_houseList(self, response):
        self.logger.debug("解析房屋列表")
        houseList = response.xpath("//div[@class='houseList']//p[@class='title']//a/@href").extract()
        for item in houseList:
            url = response.urljoin(item)
            yield  Request(url=url, callback=self.parse_houseInfo)

    def str_strip(self,str):
        result = ''
        if(str):
            result = str.replace('\r','').replace('\n','').strip()
        return result


    def parse_houseInfo(self, response):
        self.logger.debug("解析房屋详情")
        huxing = response.xpath("//div[@class='tt']/text()").extract()[0]
        mianji = response.xpath("//div[@class='tt']/text()").extract()[1]
        danjia = response.xpath("//div[@class='tt']/text()").extract()[2]
        chaoxiang = response.xpath("//div[@class='tt']/text()").extract()[3]
        louceng = response.xpath("//div[@class='tt']/text()").extract()[4]
        zonglouceng = response.xpath("//div[@class='trl-item1'][2]//div[2]/text()").extract()[1]
        zhuangxiu = response.xpath("//div[@class='tt']/text()").extract()[5]
        xiaoqu = response.xpath("//a[@id='agantesfxq_C03_05']/text()").extract_first()
        qu = response.xpath("//a[@id='agantesfxq_C03_07']/text()").extract_first()
        if(qu[len(qu)-1])!='区':
            qu = qu + '区'
        pian = response.xpath("//a[@id='agantesfxq_C03_08']/text()").extract_first()
        xuexiao = response.xpath("//a[@id='agantesfxq_C03_09']/text()").extract_first()

        niandai = response.xpath("//span[text()='建筑年代']/parent::*/span[@class='rcont']/text()").extract_first()
        dianti = response.xpath("//span[text()='有无电梯']/parent::*/span[@class='rcont']/text()").extract_first()
        chanquan = response.xpath("//span[text()='产权性质']/parent::*/span[@class='rcont']/text()").extract_first()
        zhuzhaileibie = response.xpath("//span[text()='住宅类别']/parent::*/span[@class='rcont']/text()").extract_first()
        jianzhujiegou = response.xpath("//span[text()='建筑结构']/parent::*/span[@class='rcont']/text()").extract_first()
        jianzhuleibie = response.xpath("//span[text()='建筑类别']/parent::*/span[@class='rcont']/text()").extract_first()
        jianzhuxingshi = response.xpath("//span[text()='建筑形式']/parent::*/span[@class='rcont']/text()").extract_first()
        guapai = response.xpath("//span[text()='挂牌时间']/parent::*/span[@class='rcont']/text()").extract_first()\
            .strip('(')
        gengxin = response.xpath("//i[@id='Time']/text()").extract_first()

        junjia = response.xpath("//span[text()='参考均价']/parent::*/span[@class='rcont']/i/text()").extract_first()
        leixing = response.xpath("//span[text()='物业类型']/parent::*/span[@class='rcont']/text()").extract_first()
        wuyefei = response.xpath("//span[text()='物业费用']/parent::*/span[@class='rcont']/text()").extract_first()
        jianzhu = response.xpath("//span[text()='建筑类型']/parent::*/span[@class='rcont']/text()").extract_first()
        nianxian = response.xpath("//span[text()='产权年限']/parent::*/span[@class='rcont']/text()").extract_first()
        lvhua = response.xpath("//span[text()='绿  化  率']/parent::*/span[@class='rcont']/text()").extract_first()
        rongji = response.xpath("//span[text()='容  积  率']/parent::*/span[@class='rcont']/text()").extract_first()
        fenliu = response.xpath("//span[text()='人车分流']/parent::*/span[@class='rcont']/text()").extract_first()
        loudong = response.xpath("//span[text()='总楼栋数']/parent::*/span[@class='rcont']/text()").extract_first()
        hushu = response.xpath("//span[text()='总  户  数']/parent::*/span[@class='rcont']/text()").extract_first()

        yield {
            'chengshi': '武汉',
            'huxing': self.str_strip(huxing),
            'mianji': self.str_strip(mianji),
            'danjia': self.str_strip(danjia),
            'chaoxiang': self.str_strip(chaoxiang),
            'louceng': self.str_strip(louceng),
            'zonglouceng': self.str_strip(zonglouceng),
            'zhuangxiu': self.str_strip(zhuangxiu),
            'xiaoqu': self.str_strip(xiaoqu),
            'qu': self.str_strip(qu),
            'pian': self.str_strip(pian),
            'xuexiao': self.str_strip(xuexiao),
            'niandai': self.str_strip(niandai),
            'dianti': self.str_strip(dianti),
            'chanquan': self.str_strip(chanquan),
            'zhuzhaileibie': self.str_strip(zhuzhaileibie),
            'jianzhujiegou': self.str_strip(jianzhujiegou),
            'jianzhuleibie': self.str_strip(jianzhuleibie),
            'jianzhuxingshi': self.str_strip(jianzhuxingshi),
            'guapai': self.str_strip(guapai),
            'gengxin': self.str_strip(gengxin),
            'junjia': self.str_strip(junjia),
            'leixing': self.str_strip(leixing),
            'wuyefei': self.str_strip(wuyefei),
            'jianzhu': self.str_strip(jianzhu),
            'nianxian': self.str_strip(nianxian),
            'lvhua': self.str_strip(lvhua),
            'rongji': self.str_strip(rongji),
            'fenliu': self.str_strip(fenliu),
            'loudong': self.str_strip(loudong),
            'hushu': self.str_strip(hushu),
            'url': response.url,
            'laiyuan': 'fang.com'
        }

        # url_xiaoqu = response.xpath("//a[@id='agantesfxq_C03_05']/@href").extract_first()
        # url_xiaoqu_xiangqing = url_xiaoqu + 'xiangqing/'
        # yield Request(url=url_xiaoqu_xiangqing,callback=self.parse_xiaoqu)

    def parse_xiaoqu(self, response):
        '''
        小区地址：江岸区塔子湖西路1号
        所属区域：江岸区 后湖
        邮    编：430014
        环线位置：二至三环
        产权描述：住宅、洋房产权为70年；LOFT商住楼产权...
        物业类别：住宅
        竣工时间：2013-08-01
        开 发 商：武汉星科房地产开发有限公司
        建筑结构：框架、钢筋混凝土
        建筑类别：板楼 多层 小高层 高层 超高层
        建筑面积：1844270平方米
        占地面积：459490平方米
        当期户数：1722户
        总 户 数：1722户
        绿 化 率：35%
        容 积 率：3.54
        物业办公电话：027-88614519
        物 业 费：1.80元/平米·月
        附加信息：物业费1.8元/平米·月
        物业办公地点：4栋1楼、6栋1楼、7栋1楼
        '''


        info = response.xpath("//div[@class='inforwrap clearfix']//dd/text()").extract()
        xipaoqudizhi = info[0]
        suoshuquyu = info[1]
        youbian = info[2]
        huanxianweizhi = info[3]
        chanquan = info[4]
        wuyeleibie = info[5]
        jungongshijian = info[6]
        kaifashang = info[7]
        jianzhujiegou = info[8]
        jianzhuleibie = info[9]
        jianzhumianji = info[10]
        zhandimianji = info[11]
        dangqihushu = info[12]
        zonghushu = info[13]
        lvhua = info[14]
        rongji = info[15]
        wuyedianhua = info[16]
        wuyefei = info[17]
        fujiaxinxi = info[18]
        wuyedizhi = info[19]
