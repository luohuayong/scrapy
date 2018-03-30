#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by Leo on 2017/11/16

"""
代码说明：
"""
from house.pg_helper import Pg_helper
import logging

from datetime import datetime,timedelta

class Data_helper(object):

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

    def get_laiyuan_dict(self):
        result = {}
        sql = "select * from house_laiyuan;"
        rows = self.pg_helper.readDb(sql)
        for item in rows:
            result[item['name']] = item['id']
        return result

    def __init__(self):
        self.logger = logging.getLogger(name=__name__)
        self.pg_helper = Pg_helper()

        self.city_dict = self.get_city_dict()
        self.huxing_dict = self.get_huxing_dict()
        self.chaoxiang_dict = self.get_chaoxiang_dict()
        self.louceng_list = self.get_louceng_list()
        self.chanquan_dict = self.get_chanquan_dict()
        self.jianzhu_dict = self.get_jianzhu_dict()
        self.laiyuan_dict = self.get_laiyuan_dict()

    # def get_or_insert_qu(self, city_name, qu_name):
    #     city_id = self.get_city_id(city_name)
    #
    #     sql = "select id from house_qu where city_id={0} and name='{1}';". \
    #         format(city_id, qu_name)
    #     rows = self.pg_helper.readDb(sql)
    #     if len(rows) == 0:
    #         sql = "insert into house_qu (city_id,name) values ({0},'{1}') returning id;". \
    #             format(city_id,qu_name)
    #         rows = self.pg_helper.readDb(sql)
    #     return rows[0]['id']
    #
    # def get_or_insert_pian(self, city_name, qu_name, pian_name):
    #     qu_id = self.get_or_insert_qu(city_name, qu_name)
    #     sql = "select id from house_pian where qu_id={0} and name='{1}';". \
    #         format(qu_id, pian_name)
    #     rows = self.pg_helper.readDb(sql)
    #     if len(rows) == 0:
    #         sql = "insert into house_pian (qu_id,name) values ({0},'{1}') returning id;". \
    #             format(qu_id, pian_name)
    #         rows = self.pg_helper.readDb(sql)
    #     return rows[0]['id']
    #
    # def get_or_insert_xiaoqu(self,city_name, qu_name, pian_name, xiaoqu_name):
    #     pian_id = self.get_or_insert_pian(city_name, qu_name, pian_name)
    #     sql = "select id from house_xiaoqu where pian_id={0} and name='{1}';". \
    #         format(pian_id, xiaoqu_name)
    #     rows = self.pg_helper.readDb(sql)
    #     if len(rows) == 0:
    #         sql = "insert into house_xiaoqu (pian_id,name) values ({0},'{1}') returning id;". \
    #             format(pian_id, xiaoqu_name)
    #         rows = self.pg_helper.readDb(sql)
    #     return rows[0]['id']
    #
    # def get_or_insert_xuexiao(self,city_name, xuexiao_name):
    #     sql = "select id from house_city where name='{0}';".format(city_name)
    #     rows = self.pg_helper.readDb(sql)
    #     if len(rows) == 0:
    #         raise Exception('城市名称不存在 - {0}'.format(city_name))
    #     city_id = rows[0]['id']
    #
    #     sql = "select id from house_xuexiao where city_id={0} and name='{1}';". \
    #         format(city_id, xuexiao_name)
    #     rows = self.pg_helper.readDb(sql)
    #     if len(rows) == 0:
    #         sql = "insert into house_xuexiao (city_id,name) values ({0},'{1}') returning id;". \
    #             format(city_id, xuexiao_name)
    #         rows = self.pg_helper.readDb(sql)
    #     return rows[0]['id']

    def get_city_id(self,city_name):
        if city_name in self.city_dict:
            return self.city_dict[city_name]
        else:
            return None
            # raise Exception('城市错误 - {0}'.format(city_name))

    def get_qu_id(self, city_id, qu_name):
        sql = "select id from house_qu where city_id={0} and name='{1}';". \
            format(city_id, qu_name)
        rows = self.pg_helper.readDb(sql)
        if len(rows) == 1:
            return rows[0]['id']

        sql = ("select id from house_qu where city_id={0}"\
              " and (position(name in '{1}')>0 or position('{1}' in name)>0);").\
            format(city_id,qu_name)
        rows = self.pg_helper.readDb(sql)
        if len(rows) == 1:
            return rows[0]['id']
        return None

    def insert_qu(self, city_id, qu_name):
        sql = "insert into house_qu (city_id,name) values ({0},'{1}') returning id;". \
            format(city_id,qu_name)
        rows = self.pg_helper.readDb(sql)
        return rows[0]['id']

    def get_pian_id(self, qu_id, pian_name):
        sql = "select id from house_pian where qu_id={0} and name='{1}';". \
            format(qu_id, pian_name)
        rows = self.pg_helper.readDb(sql)
        if len(rows) == 1:
            return rows[0]['id']

        sql = ("select id from house_pian where qu_id={0}"
               " and position('{1}' in name)>0;"). \
            format(qu_id, pian_name)
        rows = self.pg_helper.readDb(sql)
        if len(rows) == 1:
            return rows[0]['id']
        else:
            return None

    def insert_pian(self, qu_id, pian_name, laiyuan_id):
        sql = "insert into house_pian (qu_id,name,laiyuan_id) values ({0},'{1}',{2}) returning id;". \
            format(qu_id, pian_name, laiyuan_id)
        rows = self.pg_helper.readDb(sql)
        return rows[0]['id']

    def get_xiaoqu_id(self, qu_id, xiaoqu_name):
        sql = "select id from house_xiaoqu where qu_id={0} and name='{1}';". \
            format(qu_id, xiaoqu_name)
        rows = self.pg_helper.readDb(sql)
        if len(rows) == 1:
            return rows[0]['id']

        sql = ("select id from house_xiaoqu where qu_id={0}"
               " and position(name in '{1}')>0;"). \
            format(qu_id, xiaoqu_name)
        rows = self.pg_helper.readDb(sql)
        if len(rows) == 1:
            return rows[0]['id']
        else:
            return None


    def get_xiaoqu_by_city(self,city_name,xiaoqu_name):
        sql = ("select xiaoqu.id as xiaoqu_id,xiaoqu.name as xiaoqu_name,"
               "pian.id as pian_id,pian.name as pian_name,"
               "qu.id as qu_id,qu.name as qu_name,"
               "city.id as city_id,city.name as city_name"
               " from house_xiaoqu xiaoqu"
               " left join house_pian pian on xiaoqu.pian_id = pian.id"
               " left join house_qu qu on pian.qu_id = qu.id"
               " left join house_city city on qu.city_id = city.id"
               " where xiaoqu.name = '{0}' and city.name = '{1}'")
        sql = sql.format(xiaoqu_name,city_name)
        rows = self.pg_helper.readDb(sql)
        if len(rows) == 0:
            return None
        else:
            return rows[0]
            # rows[0]['city_id'],rows[0]['qu_id'],rows[0]['pian_id'],rows[0]['xiaoqu_id']

    def insert_xiaoqu(self,city_id,qu_id,pian_id, xiaoqu_name,laiyuan_id):
        if (xiaoqu_name==None or xiaoqu_name == ''):
            raise Exception('小区名错误 - {0}'.format(xiaoqu_name))
        sql = ("insert into house_xiaoqu (city_id,qu_id,pian_id,name,laiyuan_id) "
               "values ({0},{1},{2},'{3}',{4}) returning id;"). \
            format(city_id, qu_id, pian_id, xiaoqu_name, laiyuan_id)
        rows = self.pg_helper.readDb(sql)
        return rows[0]['id']

    def insert_xiaoqu_bieming(self, city_name, city_id, bieming):
        sql = "select * from house_xiaoqu_bieming where city_name='{0}' and bieming='{1}';".\
            format(city_name,bieming)
        rows=self.pg_helper.readDb(sql)
        if(len(rows)>0):
            return rows[0]['id']

        sql = "select * from house_xiaoqu where city_id={0} and name='{1}';".\
            format(city_id,bieming)
        rows=self.pg_helper.readDb(sql)
        if(len(rows)!=1):
            sql = "select * from house_xiaoqu where city_id={0} and position(name in '{1}')>0;". \
                format(city_id,bieming)
            rows=self.pg_helper.readDb(sql)
        if(len(rows)!=1):
            sql = "select * from house_xiaoqu where city_id={0} and position('{1}' in name)>0;". \
                format(city_id,bieming)
            rows=self.pg_helper.readDb(sql)
        if(len(rows)==1):
            sql = ("insert into house_xiaoqu_bieming(city_name,bieming,xiaoqu_name,xiaoqu_id)"
                   " values('{0}','{1}','{2}',{3});"). \
                format(city_name, bieming, rows[0]['name'], rows[0]['id'])
            rows=self.pg_helper.writeDb(sql)
        else:
            sql = ("insert into house_xiaoqu_bieming(city_name,bieming)"
                   " values('{0}','{1}');"). \
                format(city_name, bieming)
            rows=self.pg_helper.writeDb(sql)
        return rows[0]['id']

    def get_xuexiao_id(self,city_id,xuexiao_name):
        sql = "select id from house_xuexiao where city_id={0} and name='{1}';". \
            format(city_id, xuexiao_name)
        rows = self.pg_helper.readDb(sql)
        if len(rows) == 0:
            return None
        else:
            return rows[0]['id']

    def insert_xuexiao(self,city_id,xuexiao_name):
        sql = "insert into house_xuexiao (city_id,name) values ({0},'{1}') returning id;". \
            format(city_id, xuexiao_name)
        rows = self.pg_helper.readDb(sql)
        return rows[0]['id']

    def get_huxing_id(self,huxing_name):
        if huxing_name in self.huxing_dict:
            return self.huxing_dict[huxing_name]
        else:
            return None

    def get_chaoxiang_id(self,chaoxiang_name):
        if chaoxiang_name in self.chaoxiang_dict:
            return self.chaoxiang_dict[chaoxiang_name]
        else:
            return None


    def get_louceng_id(self, louceng, zonglouceng):
        for item in self.louceng_list:
            if (louceng==item['lou'] and item['min']<zonglouceng and item['max']>=zonglouceng):
                return item['id']
        return None


    def get_chanquan_id(self,chanquan_name):
        if chanquan_name in self.chanquan_dict:
            return self.chanquan_dict[chanquan_name]
        else:
            return None
            # raise Exception('产权错误 - {0}'.format(chanquan_name))

    def get_jianzhu_id(self,jianzhu_name):
        if jianzhu_name in self.jianzhu_dict:
            return self.jianzhu_dict[jianzhu_name]
        else:
            return None
            # raise Exception('建筑类型错误 - {0}'.format(jianzhu_name))

    def get_laiyuan_id(self,laiyuan_name):
        if laiyuan_name in self.laiyuan_dict:
            return self.laiyuan_dict[laiyuan_name]
        else:
            return None
            # raise Exception('来源错误 - {0}'.format(laiyuan_name))

    def isnumber(self,str):
        try:
            float(str)
            return True
        except:
            return False

if __name__ == '__main__':
    pg_helper = Pg_helper()
    data_helper = Data_helper()
    sql = "select * from house_xiaoqu"
    rows = pg_helper.readDb(sql)
    for item in rows:
        data_helper.insert_xiaoqu_bieming('武汉',360,item['name'])
