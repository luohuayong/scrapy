#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by Leo on 2017/11/9

"""
代码说明：
"""
from pg_helper import Pg_helper
import logging
import re
from datetime import datetime,timedelta

class Fangwu_helper(object):
    pg_helper = Pg_helper()

    def get_city_dict(self):
        result = {}
        sql = "select * from house_city;"
        rows = self.pg_helper.readDb(sql)
        for item in rows:
            result[item['name']] = item['id']
        return result

    def get_huxing_dict(self):
        result = {}
        sql = "select * from house_huxing;"
        rows = self.pg_helper.readDb(sql)
        for item in rows:
            result[item['name']] = item['id']
        return result

    def get_chaoxiang_dict(self):
        result = {}
        sql = "select * from house_chaoxiang;"
        rows = self.pg_helper.readDb(sql)
        for item in rows:
            result[item['name']] = item['id']
        return result

    def get_louceng_list(self):
        sql = "select * from house_louceng;"
        rows = self.pg_helper.readDb(sql)
        return rows

    def get_chanquan_dict(self):
        result = {}
        sql = "select * from house_chanquan;"
        rows = self.pg_helper.readDb(sql)
        for item in rows:
            result[item['name']] = item['id']
        return result

    def get_jianzhu_dict(self):
        result = {}
        sql = "select * from house_jianzhu;"
        rows = self.pg_helper.readDb(sql)
        for item in rows:
            result[item['name']] = item['id']
        return result

    def __init__(self):
        self.logger = logging.getLogger(name=__name__)

        self.city_dict = self.get_city_dict()
        self.huxing_dict = self.get_huxing_dict()
        self.chaoxiang_dict = self.get_chaoxiang_dict()
        self.louceng_list = self.get_louceng_list()
        self.chanquan_dict = self.get_chanquan_dict()
        self.jianzhu_dict = self.get_jianzhu_dict()

    def get_or_insert_qu(self, city_name, qu_name):
        city_id = self.get_city_id(city_name)

        sql = "select id from house_qu where city_id={0} and name='{1}';". \
            format(city_id, qu_name)
        rows = self.pg_helper.readDb(sql)
        if len(rows) == 0:
            sql = "insert into house_qu (city_id,name) values ({0},'{1}') returning id;". \
                format(city_id,qu_name)
            rows = self.pg_helper.readDb(sql)
        return rows[0]['id']

    def get_or_insert_pian(self, city_name, qu_name, pian_name):
        qu_id = self.get_or_insert_qu(city_name, qu_name)
        sql = "select id from house_pian where qu_id={0} and name='{1}';". \
            format(qu_id, pian_name)
        rows = self.pg_helper.readDb(sql)
        if len(rows) == 0:
            sql = "insert into house_pian (qu_id,name) values ({0},'{1}') returning id;". \
                format(qu_id, pian_name)
            rows = self.pg_helper.readDb(sql)
        return rows[0]['id']

    def get_or_insert_xiaoqu(self,city_name, qu_name, pian_name, xiaoqu_name):
        pian_id = self.get_or_insert_pian(city_name, qu_name, pian_name)
        sql = "select id from house_xiaoqu where pian_id={0} and name='{1}';". \
            format(pian_id, xiaoqu_name)
        rows = self.pg_helper.readDb(sql)
        if len(rows) == 0:
            sql = "insert into house_xiaoqu (pian_id,name) values ({0},'{1}') returning id;". \
                format(pian_id, xiaoqu_name)
            rows = self.pg_helper.readDb(sql)
        return rows[0]['id']

    def get_or_insert_xuexiao(self,city_name, xuexiao_name):
        sql = "select id from house_city where name='{0}';".format(city_name)
        rows = self.pg_helper.readDb(sql)
        if len(rows) == 0:
            raise Exception('城市名称不存在 - {0}'.format(city_name))
        city_id = rows[0]['id']

        sql = "select id from house_xuexiao where city_id={0} and name='{1}';". \
            format(city_id, xuexiao_name)
        rows = self.pg_helper.readDb(sql)
        if len(rows) == 0:
            sql = "insert into house_xuexiao (city_id,name) values ({0},'{1}') returning id;". \
                format(city_id, xuexiao_name)
            rows = self.pg_helper.readDb(sql)
        return rows[0]['id']

    def get_city_id(self,city_name):
        if city_name in self.city_dict:
            return self.city_dict[city_name]
        else:
            raise Exception('城市错误 - {0}'.format(city_name))

    def get_huxing_id(self,huxing_name):
        if huxing_name in self.huxing_dict:
            return self.huxing_dict[huxing_name]
        else:
            raise Exception('户型错误 - {0}'.format(huxing_name))

    def get_chaoxiang_id(self,chaoxiang_name):
        if chaoxiang_name in self.chaoxiang_dict:
            return self.chaoxiang_dict[chaoxiang_name]
        else:
            raise Exception('朝向错误 - {0}'.format(chaoxiang_name))

    def get_louceng_id(self,louceng,zhonglouceng):
        for item in self.louceng_list:
            if (louceng==item['lou'] and item['min']<zhonglouceng and item['max']>=zhonglouceng):
                return item['id']
        raise Exception('楼层错误 - {0}／{1}'.format(louceng,zhonglouceng))

    def get_chanquan_id(self,chanquan_name):
        if chanquan_name in self.chanquan_dict:
            return self.chanquan_dict[chanquan_name]
        else:
            raise Exception('产权错误 - {0}'.format(chanquan_name))

    def get_jianzhu_id(self,jianzhu_name):
        if jianzhu_name in self.jianzhu_dict:
            return self.jianzhu_dict[jianzhu_name]
        else:
            raise Exception('建筑类型错误 - {0}'.format(jianzhu_name))

    def create_fangwu_data(self,item):
        result = {}
        city_name = item['chengshi']
        qu_name = item['qu']
        pian_name = item['pian']
        xiaoqu_name = item['xiaoqu']

        result['city_id'] = self.get_city_id(city_name)
        result['qu_id'] = self.get_or_insert_qu(city_name,qu_name)
        result['pian_id'] = self.get_or_insert_pian(city_name,qu_name,pian_name)
        result['xiaoqu_id'] = self.get_or_insert_xiaoqu(city_name, qu_name, pian_name, xiaoqu_name)

        result['huxing_id'] = self.get_huxing_id(item['huxing'])

        # p_huxing = r'\d+室\d+厅\d+卫'
        # if(re.match(p_huxing,item['huxing'])):
        #     result['huxing_id'] = self.get_huxing_id(item['huxing'])
        # else:
        #     raise Exception('无效的户型 - {0}'.format(item['huxing']))

        if(item['mianji'].find('平')>0 and self.pg_helper.isnumber(item['mianji'].split('平')[0])):
            result['mianji'] = float(item['mianji'].split('平')[0])
        else:
            raise Exception('无效的面积 - {0}'.format(item['mianji']))

        if(item['danjia'].find('元')>0 and self.pg_helper.isnumber(item['danjia'].split('元')[0])):
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
