#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by Leo on 2017/11/16

"""
代码说明：
"""
from .pg_helper import Pg_helper
from .data_helper import Data_helper
import logging
import hashlib
from datetime import datetime,timedelta
import re

class Fang_helper(object):

    def __init__(self):
        self.logger = logging.getLogger(name=__name__)
        self.pg_helper = Pg_helper()
        self.data_helper = Data_helper()

    def get_zhiwen(self,item):
        str = item['chengshi'] + item['qu'] + item['pian'] + item['xiaoqu'] \
              + item['huxing'] + item['mianji'] + item['danjia'] + item['chaoxiang'] \
              + item['louceng'] + item['zonglouceng'] + item['zhuangxiu']
        zhiwen = hashlib.md5(str.encode('utf-8')).hexdigest()
        return zhiwen

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

    def create_fangwu_data(self,item):
        result = {}

        city_name = item['chengshi']
        city_id = self.data_helper.get_city_id(city_name)
        if(not city_id):
            raise Exception('城市错误 - {0}'.format(city_name))
        result['city_id'] = city_id

        qu_name = item['qu']
        qu_id = self.data_helper.get_qu_id(city_id,qu_name)
        if(not qu_id):
            qu_id = self.data_helper.insert_qu(city_id,qu_name)
        result['qu_id'] = qu_id

        pian_name = item['pian']
        pian_id = self.data_helper.get_pian_id(qu_id,pian_name)
        if(not pian_id):
            pian_id = self.data_helper.insert_pian(qu_id,pian_name)
        result['pian_id'] = pian_id

        xiaoqu_name = item['xiaoqu']
        xiaoqu_id = self.data_helper.get_xiaoqu_id(pian_id,xiaoqu_name)
        if(not xiaoqu_id):
            xiaoqu_id = self.data_helper.insert_xiaoqu(pian_id,xiaoqu_name)
        result['xiaoqu_id'] = xiaoqu_id

        huxing_name = item['huxing']
        huxing_id = self.data_helper.get_huxing_id(huxing_name)
        if(not huxing_id):
            raise Exception('户型错误 - {0}'.format(huxing_name))
        result['huxing_id'] = huxing_id


        if(item['mianji'].find('平')>0 and self.data_helper.isnumber(item['mianji'].split('平')[0])):
            result['mianji'] = float(item['mianji'].split('平')[0])
        else:
            raise Exception('无效的面积 - {0}'.format(item['mianji']))

        if(item['danjia'].find('元')>0 and self.data_helper.isnumber(item['danjia'].split('元')[0])):
            result['danjia'] = float(item['danjia'].split('元')[0])
        else:
            raise Exception('无效的单价 - {0}'.format(item['danjia']))

        chaoxiang_name = item['chaoxiang']
        chaoxiang_id = self.data_helper.get_chaoxiang_id(chaoxiang_name)
        if(not chaoxiang_id):
            raise Exception('朝向错误 - {0}'.format(chaoxiang_name))
        result['chaoxiang_id'] = chaoxiang_id

        zonglouceng = int(item['zonglouceng'].split('共')[1].split('层')[0])
        louceng = item['louceng']
        louceng_id = self.data_helper.get_louceng_id(louceng,zonglouceng)
        if(not louceng_id):
            raise Exception('楼层错误 - {0}／{1}'.format(louceng,zonglouceng))
        result['louceng_id'] = louceng_id


        if(item['niandai'].find('年')>0 and item['niandai'].split('年')[0].isnumeric()):
            result['niandai'] = int(item['niandai'].split('年')[0])
        else:
            raise Exception('无效的年代 - {0}'.format(item['niandai']))

        chanquan_id = None
        if item['nianxian'] != '':
            nianxian = item['nianxian'][:2] + '年'
            chanquan_id = self.data_helper.get_chanquan_id(nianxian)
        elif item['zhuzhaileibie'] == '普通住宅':
            chanquan_id = self.data_helper.get_chanquan_id('70年')
        if(not chanquan_id):
            raise Exception('无效的产权年限 - {0}'.format(item['nianxian']))
        result['chanquan_id'] = chanquan_id

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

        jianzhu_name = item['jianzhu']
        jianzhu_id = self.data_helper.get_jianzhu_id(jianzhu_name)
        if(not jianzhu_id):
            jianzhu_name = item['jianzhuleibie']
            jianzhu_id = self.data_helper.get_jianzhu_id(jianzhu_name)
            if(not jianzhu_id):
                jianzhu_name = item['jianzhuxingshi']
                jianzhu_id = self.data_helper.get_jianzhu_id(jianzhu_name)
                if(not jianzhu_id):
                    raise Exception('建筑类型错误 - {0}'.format(item['jianzhu']))

        # if(item['jianzhu'] in self.jianzhu_dict):
        #     result['jianzhu_id'] = self.jianzhu_dict[item['jianzhu']]
        # elif(item['jianzhuleibie'] in self.jianzhu_dict):
        #     result['jianzhu_id'] = self.jianzhu_dict[item['jianzhuleibie']]
        # elif(item['jianzhuxingshi'] in self.jianzhu_dict):
        #     result['jianzhu_id'] = self.jianzhu_dict[item['jianzhuxingshi']]
        # else:
        #     raise Exception('建筑类型错误 - {0}'.format(item['jianzhu']))

        result['zhiwen'] = item['zhiwen']

        laiyuan_name = item['laiyuan']
        laiyuan_id = self.data_helper.get_laiyuan_id(laiyuan_name)
        if(not laiyuan_id):
            raise Exception('来源错误 - {0}'.format(laiyuan_name))
        result['laiyuan_id'] = laiyuan_id
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
                           "jianzhu_id,zhiwen,laiyuan_id) values (%(city_id)s,%(qu_id)s,%(pian_id)s,%(xiaoqu_id)s,"
                           "%(huxing_id)s,%(mianji)s,%(danjia)s,%(chaoxiang_id)s,%(louceng_id)s,"
                           "%(niandai)s,%(chanquan_id)s,%(guapai)s,%(gengxin)s,%(jianzhu_id)s,%(zhiwen)s),%(laiyuan_id)s;")
                    self.pg_helper.writeDb(sql,item_fangwu)

                sql = "update house_caiji set status='1' where id={0}".format(item['id'])
                self.pg_helper.writeDb(sql)
            except Exception as e:
                self.logger.error("%s: %s" % (e.__class__.__name__, e))

if __name__ == '__main__':
    pass