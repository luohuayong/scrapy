#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by Leo on 2017/11/10

"""
代码说明：
"""

from .pg_helper import Pg_helper
import logging
import hashlib
from datetime import datetime,timedelta

class Caiji_helper(object):
    pg_helper = Pg_helper()

    def __init__(self):
        self.logger = logging.getLogger(name=__name__)

    def insert_test(self):
        sql = ("insert into house_caiji (chengshi,qu,pian,xiaoqu,huxing,",
               "mianji,danjia,chaoxiang,louceng,zonglouceng,zhuangxiu,",
               "xuexiao,niandai,dianti,chanquan,guapai,gengxin,junjia,",
               "leixing,wuyefei,jianzhu,nianxian,lvhua,rongji,fenliu,",
               "loudong,hushu,laiyuan,zhiwen,addTime,url)",
               " values('%s','%s','%s','%s','%s',",
               "'%s','%s','%s','%s','%s','%s',",
               "'%s','%s','%s','%s','%s','%s','%s',",
               "'%s','%s','%s','%s','%s','%s','%s',",
               "'%s','%s','%s','%s','%s','%s')")
        sql = sql % ("武汉","武昌区","东湖东亭","东湖名邸","3室2厅2卫",
                     "125","20240","南北","中层","11","简装修",
                     "武昌东湖小学","2001年","有","个人产权","2017-09-04","7分钟前更新","19976",
                     "住宅","1元/平米·月","塔楼","70年","32%","3.75","无",
                     "1","176","fang.com","1234567890ABCDEF","2017-10-30 10:20:45.51225",
                     "http://esf.wuhan.fang.com/chushou/3_167680394.htm",)
        self.pg_helper.writeDb(sql)

    def insert_caiji(self,item):
        try:
            zhiwen = self.get_zhiwen(item)
            create_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            item['create_date'] = create_date
            item['zhiwen'] = zhiwen
            item['status'] = '0'
            sql = "select * from house_caiji where zhiwen='{0}';".format(zhiwen)
            rows = self.pg_helper.readDb(sql)
            if len(rows) > 0:
                self.logger.info("house_caiji数据已存在")
                return
            self.pg_helper.insert(item,"house_caiji")
        except Exception as e:
            self.logger.error("%s: %s" % (e.__class__.__name__, e))


    def get_zhiwen(self,item):
        str = item['chengshi'] + item['qu'] + item['pian'] + item['xiaoqu'] \
              + item['huxing'] + item['mianji'] + item['danjia'] + item['chaoxiang'] \
              + item['louceng'] + item['zonglouceng'] + item['zhuangxiu']
        zhiwen = hashlib.md5(str.encode('utf-8')).hexdigest()
        return zhiwen

if __name__ == '__main__':
    h = Pg_helper()
    # h.insert_tf_data()
    # h.insert_test()
    create_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    item = {
        'chengshi' : "武汉",
        'qu' : "武昌区",
        'pian' : "东湖东亭",
        'xiaoqu' : "东湖名邸",
        'huxing' : "3室2厅2卫",
        'mianji' : "125",
        'danjia' : "20240",
        'chaoxiang' : "南北",
        'louceng': "中层",
        'zonglouceng' : "11",
        'zhuangxiu' : "简装修",
        'xuexiao' : "武昌东湖小学",
        'niandai' : "2001年",
        'dianti' : "有",
        'chanquan' : "个人产权",
        'guapai': "2017-09-04",
        'gengxin' : "7分钟前更新",
        'junjia' : "19976",
        'leixing' : "住宅",
        'wuyefei' : "1元/平米·月",
        'jianzhu': "塔楼",
        'nianxian' : "70年",
        'lvhua' : "32%",
        'rongji' : "3.75",
        'fenliu' : "无",
        'loudong': "1",
        'hushu': "176",
        'laiyuan': "fang.com",
        'zhiwen' : "1234567890ABCDEF",
        'create_date' : create_date,
        'url': "http://esf.wuhan.fang.com/chushou/3_167680394.htm",
    }

    h.insert(item,"house_caiji")