#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by Leo on 2017/11/9

"""
代码说明：
"""
from house.pg_helper import Pg_helper
from house.data_helper import Data_helper
import logging
import hashlib
import re
from datetime import datetime,timedelta

class Lianjia_helper(object):

    def __init__(self):
        self.logger = logging.getLogger(name=__name__)
        self.data_helper = Data_helper()
        self.pg_helper = Pg_helper()

    def get_zhiwen(self,item):
        str = item['chengshi'] + item['qu'] + item['pian'] + item['xiaoqu'] \
              + item['huxing'] + item['mianji'] + item['danjia'] + item['chaoxiang'] \
              + item['louceng'] + item['zonglouceng'] + item['zhuangxiu']
        zhiwen = hashlib.md5(str.encode('utf-8')).hexdigest()
        return zhiwen

    def insert_xiaoqu(self,item):
        laiyuan_id = self.data_helper.get_laiyuan_id('lianjia.com')

        city_name = item['city_name']
        city_id = self.data_helper.get_city_id(city_name)
        if(not city_id):
            raise Exception('城市错误 - {0}'.format(city_name))

        qu_name = item['qu_name']
        qu_id = self.data_helper.get_qu_id(city_id,qu_name)
        if(not qu_id):
            raise Exception('区错误 - {0}'.format(qu_name))

        pian_name = item['pian_name']
        pian_id = self.data_helper.get_pian_id(qu_id,pian_name)
        if(not pian_id):
            pian_id = self.data_helper.insert_pian(qu_id,pian_name,laiyuan_id)

        xiaoqu_name = item['xiaoqu_name']
        xiaoqu_id = self.data_helper.get_xiaoqu_id(qu_id,xiaoqu_name)
        if(not xiaoqu_id):
            xiaoqu_id = self.data_helper.insert_xiaoqu(city_id,qu_id,pian_id,xiaoqu_name,laiyuan_id)
        return xiaoqu_id

    def create_fangwu_data(self,item):
        result = {}

        city_name = item['chengshi']
        xiaoqu_name = item['xiaoqu']
        row = self.data_helper.get_xiaoqu_by_city(city_name,xiaoqu_name)
        if(not row):
            xiaoqu_id = self.data_helper.insert_xiaoqu_bieming(city_name,xiaoqu_name)
            if(not xiaoqu_id):
                raise Exception('未匹配到的小区 - {0}/{1}'.format(city_name,xiaoqu_name))
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

        if(item['mianji'].find('㎡')>0 and self.data_helper.isnumber(item['mianji'].split('㎡')[0])):
            result['mianji'] = float(item['mianji'].split('㎡')[0])
        else:
            raise Exception('无效的面积 - {0}'.format(item['mianji']))

        if(self.data_helper.isnumber(item['danjia'])):
            result['danjia'] = float(item['danjia'])
        else:
            raise Exception('无效的单价 - {0}'.format(item['danjia']))

        chaoxiang_name = item['chaoxiang']
        chaoxiang_id = self.data_helper.get_chaoxiang_id(chaoxiang_name)
        if(not chaoxiang_id):
            raise Exception('朝向错误 - {0}'.format(chaoxiang_name))
        result['chaoxiang_id'] = chaoxiang_id

        zonglouceng = int(item['zonglouceng'].split('共')[1].split('层')[0])
        louceng = item['louceng'].replace('楼','')
        louceng_id = self.data_helper.get_louceng_id(louceng,zonglouceng)
        if(not louceng_id):
            raise Exception('楼层错误 - {0}／{1}'.format(louceng,zonglouceng))
        result['louceng_id'] = louceng_id

        if(item['niandai'].find('年')>0 and item['niandai'].split('年')[0].isnumeric()):
            result['niandai'] = int(item['niandai'].split('年')[0])
        else:
            raise Exception('无效的年代 - {0}'.format(item['niandai']))

        nianxian = item['nianxian']
        chanquan_id = self.data_helper.get_chanquan_id(nianxian)
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
            raise Exception('建筑类型错误 - {0}'.format(item['jianzhu']))
        result['jianzhu_id'] = jianzhu_id

        result['zhiwen'] = item['zhiwen']

        laiyuan_name = item['laiyuan']
        laiyuan_id = self.data_helper.get_laiyuan_id(laiyuan_name)
        if(not laiyuan_id):
            raise Exception('来源错误 - {0}'.format(laiyuan_name))
        result['laiyuan_id'] = laiyuan_id

        return result

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

    def insert_fangwu(self):
        sql = "select * from house_caiji where status='0' and laiyuan='lianjia.com'"
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
                           "%(niandai)s,%(chanquan_id)s,%(guapai)s,%(gengxin)s,%(jianzhu_id)s,%(zhiwen)s,%(laiyuan_id)s);")
                    self.pg_helper.writeDb(sql,item_fangwu)

                sql = "update house_caiji set status='1' where id={0}".format(item['id'])
                self.pg_helper.writeDb(sql)
            except Exception as e:
                self.logger.error("%s: %s" % (e.__class__.__name__, e))


if __name__ == '__main__':
    lianjia_helper = Lianjia_helper()
    lianjia_helper.insert_fangwu()
