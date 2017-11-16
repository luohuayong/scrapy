#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by Leo on 2017/11/9

"""
代码说明：
"""
from .pg_helper import Pg_helper
from .data_helper import Data_helper
import logging
import hashlib
import re
from datetime import datetime,timedelta

class Lianjia_helper(object):

    def __init__(self):
        self.logger = logging.getLogger(name=__name__)
        self.pg_helper = Pg_helper()

    def get_zhiwen(self,item):
        str = item['chengshi'] + item['qu'] + item['pian'] + item['xiaoqu'] \
              + item['huxing'] + item['mianji'] + item['danjia'] + item['chaoxiang'] \
              + item['louceng'] + item['zonglouceng'] + item['zhuangxiu']
        zhiwen = hashlib.md5(str.encode('utf-8')).hexdigest()
        return zhiwen

    def create_fangwu_data(self,item):
        result = {}

        city_name = item['chengshi']
        xiaoqu_name = item['xiaoqu']
        row = self.data_helper.get_xiaoqu_by_city(city_name,xiaoqu_name)
        if(not row):
            raise Exception('小区不存在 - {0}/{1}'.format(city_name,xiaoqu_name))
        city_id = row['city_id']
        qu_id = row['qu_id']
        pian_id = row['pian_id']
        xiaoqu_id = row['xiaoqu_id']

        result['city_id'] = city_id
        result['qu_id'] = qu_id
        result['pian_id'] = pian_id
        result['xiaoqu_id'] = xiaoqu_id
        # p_huxing = r'\d+室\d+厅\d+卫'
        # if(re.match(p_huxing,item['huxing'])):
        #     result['huxing_id'] = self.get_huxing_id(item['huxing'])
        # else:
        #     raise Exception('无效的户型 - {0}'.format(item['huxing']))

        huxing_name = item['huxing']
        huxing_id = self.data_helper.get_huxing_id(huxing_name)
        if(not huxing_id):
            raise Exception('户型错误 - {0}'.format(huxing_name))
        result['huxing_id'] = huxing_id


        if(item['mianji'].find('m')>0 and self.data_helper.isnumber(item['mianji'].split('m')[0])):
            result['mianji'] = float(item['mianji'].split('m')[0])
        else:
            raise Exception('无效的面积 - {0}'.format(item['mianji']))

        if(item['danjia'].find('元')>0 and self.data_helper.isnumber(item['danjia'].split('元')[0])):
            result['danjia'] = float(item['danjia'].split('元')[0])
        else:
            raise Exception('无效的单价 - {0}'.format(item['danjia']))

        result['chaoxiang_id'] = self.get_chaoxiang_id(item['chaoxiang'])
        zonglouceng = int(item['zonglouceng'].split('共')[1].split('层')[0])
        result['louceng_id'] = self.get_louceng_id(item['louceng'],zonglouceng)

        if(item['niandai'].find('年')>0 and item['niandai'].split('年')[0].isnumeric()):
            result['niandai'] = int(item['niandai'].split('年')[0])
        else:
            raise Exception('无效的年代 - {0}'.format(item['niandai']))

        if item['nianxian'] != '':
            nianxian = item['nianxian'][:2] + '年'
            result['chanquan_id'] = self.get_chanquan_id(nianxian)
        elif item['zhuzhaileibie'] == '普通住宅':
            result['chanquan_id'] = self.get_chanquan_id('70年')
        else:
            raise Exception('无效的产权年限 - {0}'.format(item['nianxian']))

        p_guapai = r'20\d{2}-\d{2}-\d{2}'
        if(re.match(p_guapai,item['guapai'])):
            result['guapai'] = item['guapai']
        else:
            raise Exception('无效的挂牌日期 - {0}'.format(item['guapai']))

        if(item['gengxin'].find('天')>0):
            tian = int(item['gengxin'].split('天')[0])
            td = timedelta(days=tian)
            result['gengxin'] = (item['create_date'] - td).strftime('%Y-%m-%d')
        else:
            result['gengxin'] = item['create_date'].strftime('%Y-%m-%d')

        if(item['jianzhu'] in self.jianzhu_dict):
            result['jianzhu_id'] = self.jianzhu_dict[item['jianzhu']]
        elif(item['jianzhuleibie'] in self.jianzhu_dict):
            result['jianzhu_id'] = self.jianzhu_dict[item['jianzhuleibie']]
        elif(item['jianzhuxingshi'] in self.jianzhu_dict):
            result['jianzhu_id'] = self.jianzhu_dict[item['jianzhuxingshi']]
        else:
            raise Exception('建筑类型错误 - {0}'.format(item['jianzhu']))

        result['zhiwen'] = item['zhiwen']
        return result

    def insert_from_caiji(self):
        sql = "select * from house_caiji where status='0'"
        rows = self.pg_helper.readDb(sql)
        for item in rows:
            try:
                sql = "select * from house_fangwu where zhiwen='{0}';".format(item['zhiwen'])
                if(len(self.pg_helper.readDb(sql)) > 0):
                    self.logger.info("house_fangwu数据已存在")
                else:
                    item_fangwu = self.create_fangwu_data(item)
                    sql = ("insert into house_fangwu (city_id,qu_id,pian_id,xiaoqu_id,huxing_id,"
                           "mianji,danjia,chaoxiang_id,louceng_id,niandai,chanquan_id,guapai,gengxin,"
                           "jianzhu_id,zhiwen) values (%(city_id)s,%(qu_id)s,%(pian_id)s,%(xiaoqu_id)s,"
                           "%(huxing_id)s,%(mianji)s,%(danjia)s,%(chaoxiang_id)s,%(louceng_id)s,"
                           "%(niandai)s,%(chanquan_id)s,%(guapai)s,%(gengxin)s,%(jianzhu_id)s,%(zhiwen)s);")
                    self.pg_helper.writeDb(sql,item_fangwu)

                sql = "update house_caiji set status='1' where id={0}".format(item['id'])
                self.pg_helper.writeDb(sql)
            except Exception as e:
                self.logger.error("%s: %s" % (e.__class__.__name__, e))


if __name__ == '__main__':
    fangwu_helper = Fangwu_helper()
    fangwu_helper.insert_from_caiji()
