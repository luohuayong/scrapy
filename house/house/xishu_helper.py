#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by Leo on 2017/9/12

"""
代码说明：
"""
from house.pg_helper import Pg_helper
import psycopg2
import logging
import hashlib
import psycopg2.extensions
import re
import psycopg2.extras

from datetime import datetime,timedelta

class Xishu_helper(object):

    def __init__(self):
        self.pg_helper = Pg_helper()
        self.logger = logging.getLogger(name=__name__)

    def set_danjia(self):
        self.logger.debug('更新小区单价...')
        sql = ("update house_xiaoqu as hx set danjia = (select round(sum(danjia)/count(*)) "
               "from house_fangwu where xiaoqu_id = hx.id)")
        self.writeDb(sql)

        self.logger.debug('更新片单价...')
        sql = ("update house_pian as hp set danjia = (select round(sum(danjia)/count(*)) "
               "from house_fangwu where pian_id = hp.id)")
        self.writeDb(sql)

        self.logger.debug('更新区单价...')
        sql = ("update house_qu as hq set danjia = (select round(sum(danjia)/count(*)) "
               "from house_fangwu where qu_id = hq.id)")
        self.writeDb(sql)

        self.logger.debug('更新城市单价...')
        sql = ("update house_city as hc set danjia = (select round(sum(danjia)/count(*)) "
               "from house_fangwu where city_id = hc.id)")
        self.writeDb(sql)

        self.logger.debug('更新系统单价...')
        sql = ("update house_sys set value = (select round(sum(danjia)/count(*)) "
               "from house_fangwu) where name='danjia'")
        self.writeDb(sql)

        self.logger.debug('更新产权单价...')
        sql = ("update house_chanquan as h set danjia = (select round(sum(danjia)/count(*)) "
               "from house_fangwu where chanquan_id = h.id)")
        self.writeDb(sql)

        self.logger.debug('更新城市产权单价...')
        sql = ("")
        self.writeDb(sql)

        self.logger.debug('更新户型单价...')
        sql = ("update house_huxing as h set danjia = (select round(sum(danjia)/count(*)) "
               "from house_fangwu where huxing_id = h.id)")
        self.writeDb(sql)

        self.logger.debug('更新城市户型单价...')
        sql = ("")
        self.writeDb(sql)

        self.logger.debug('更新朝向单价...')
        sql = ("update house_chaoxiang as h set danjia = (select round(sum(danjia)/count(*)) "
               "from house_fangwu where chaoxiang_id = h.id)")
        self.writeDb(sql)

        self.logger.debug('更新城市朝向单价...')
        sql = ("")
        self.writeDb(sql)

        self.logger.debug('更新楼层单价...')
        sql = ("update house_louceng as h set danjia = (select round(sum(danjia)/count(*)) "
               "from house_fangwu where louceng_id = h.id)")
        self.writeDb(sql)

        self.logger.debug('更新城市楼层单价...')
        sql = ("")
        self.writeDb(sql)








    def hello(self):
        print ('hello,this is pg_helper')

if __name__ == '__main__':
    pass