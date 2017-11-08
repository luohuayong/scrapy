#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by Leo on 2017/9/12

"""
代码说明：
"""

import psycopg2
import logging
import hashlib
import psycopg2.extensions
import re
import psycopg2.extras

from datetime import datetime,timedelta

class MyCursor(psycopg2.extensions.cursor):
    def execute(self, sql, args=None):
        logger = logging.getLogger(name=__name__)

        try:
            psycopg2.extensions.cursor.execute(self, sql, args)
        except Exception as e:
            logger.error("%s: %s" % (e.__class__.__name__, e))
            raise


class Pg_helper(object):

    def __init__(self):
        self.databse = "mysite"
        self.user = "postgres"
        self.password = "123123"
        self.host = "127.0.0.1"
        self.port = "5432"
        self.logger = logging.getLogger(name=__name__)

    def writeDb(self,sql,data=None):
        conn = psycopg2.connect(database=self.databse, user=self.user,
                                password=self.password,host=self.host,port=self.port)
        cursor = conn.cursor()
        try:
            cursor.execute(sql,data)
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.error("%s: %s" % (e.__class__.__name__, e))
            return False
        finally:
            cursor.close()
            conn.close()
        return True

    def readDb(self,sql):
        conn = psycopg2.connect(database=self.databse, user=self.user,
                                password=self.password,host=self.host,port=self.port)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        data = None
        try:
            cursor.execute(sql)
            data = cursor.fetchall()
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.error("%s: %s" % (e.__class__.__name__, e))
            return False
        finally:
            cursor.close()
            conn.close()
        return data

    # def execute(self,sql,params=None):
    #     conn = psycopg2.connect(database=self.databse, user=self.user,
    #                             password=self.password,host=self.host,port=self.port)
    #     cursor = conn.cursor()
    #     try:
    #         cursor.execute(sql,params)
    #         data = cursor.fetchall()
    #         conn.commit()
    #     except Exception as e:
    #         conn.rollback()
    #         self.logging.error("%s: %s" % (e.__class__.__name__, e))
    #     finally:
    #         cursor.close()
    #         conn.close()
    #     return data

    def isnumber(self,str):
        try:
            float(str)
            return True
        except:
            return False

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
        self.writeDb(sql)

    def get_or_insert_qu(self, city_name, qu_name):
        sql = "select id from house_city where name='{0}';".format(city_name)
        rows = self.readDb(sql)
        if len(rows) == 0:
            raise Exception('城市名称不存在 - {0}'.format(city_name))
        city_id = rows[0]['id']

        sql = "select id from house_qu where city_id={0} and name='{1}';".\
            format(city_id, qu_name)
        rows = self.readDb(sql)
        if len(rows) == 0:
            sql = "insert into house_qu (city_id,name) values ({0},'{1}') returning id;".\
                format(city_id,qu_name)
            rows = self.readDb(sql)
        return rows[0]['id']

    def get_or_insert_pian(self, city_name, qu_name, pian_name):
        qu_id = self.get_or_insert_qu(city_name, qu_name)
        sql = "select id from house_pian where qu_id={0} and name='{1}';".\
            format(qu_id, pian_name)
        rows = self.readDb(sql)
        if len(rows) == 0:
            sql = "insert into house_pian (qu_id,name) values ({0},'{1}') returning id;". \
                format(qu_id, pian_name)
            rows = self.readDb(sql)
        return rows[0]['id']

    def get_or_insert_xiaoqu(self,city_name, qu_name, pian_name, xiaoqu_name):
        pian_id = self.get_or_insert_pian(city_name, qu_name, pian_name)
        sql = "select id from house_xiaoqu where pian_id={0} and name='{1}';". \
            format(pian_id, xiaoqu_name)
        rows = self.readDb(sql)
        if len(rows) == 0:
            sql = "insert into house_xiaoqu (pian_id,name) values ({0},'{1}') returning id;". \
                format(pian_id, xiaoqu_name)
            rows = self.readDb(sql)
        return rows[0]['id']

    def get_or_insert_xuexiao(self,city_name, xuexiao_name):
        sql = "select id from house_city where name='{0}';".format(city_name)
        rows = self.readDb(sql)
        if len(rows) == 0:
            raise Exception('城市名称不存在 - {0}'.format(city_name))
        city_id = rows[0]['id']

        sql = "select id from house_xuexiao where city_id={0} and name='{1}';". \
            format(city_id, xuexiao_name)
        rows = self.readDb(sql)
        if len(rows) == 0:
            sql = "insert into house_xuexiao (city_id,name) values ({0},'{1}') returning id;". \
                format(city_id, xuexiao_name)
            rows = self.readDb(sql)
        return rows[0]['id']

    def get_or_insert_huxing(self,huxing_name):
        sql = "select id from house_huxing where name='{0}';".format(huxing_name)
        rows = self.readDb(sql)
        if len(rows) == 0:
            if(huxing_name.find('室')>=0 and
                       huxing_name.find('厅')>=0 and
                       huxing_name.find('卫')>=0):
                shi = huxing_name.split('室')[0]
                ting = huxing_name.split('室')[1].split('厅')[0]
                wei = huxing_name.split('室')[1].split('厅')[1].split('卫')[0]
            else:
                shi = '0'
                ting = '0'
                wei = '0'
            sql = "insert into house_huxing (name,shi,ting,wei) values ('{0}',{1},{2},{3}) returning id;". \
                format(huxing_name,shi,ting,wei)
            rows = self.readDb(sql)
        return rows[0]['id']

    def create_fangwu_data(self,item):
        result = {}
        chaoxiang_choices = {'东': 1,'南': 2,'西': 3,'北': 4,'东南': 5,
                             '西南': 6,'西北': 7,'东北': 8,'南北': 9,'东西': 10}
        louceng_choices = {'底层': 1,'低层': 2,'中层': 3,'高层': 4,'顶层': 5}
        zhuangxiu_choices = {'毛坯': 1,'简装修': 2,'中装修': 3,'精装修': 4,'豪华装修': 5}
        dianti_choices = {'有': 1,'无': 2}
        chanquan_choices = {'个人产权': 1,'商品房': 2}
        leixing_choices = {'住宅': 1,'别墅': 2}
        jianzhu_choices = {'塔楼': 1,'板楼': 2,'板塔结合': 3,'联排': 4,'叠拼': 5,'双拼': 6,'独栋': 7}
        fenliu_choices = {'有': 1,'无': 2}

        def_mianji = 0.0
        def_danjia = 0.0
        def_huxing_name = '2室2厅1卫'
        def_chaoxiang = '南北'
        def_louceng = '中层'
        def_zonglouceng = 18
        def_zhuangxiu = '精装修'
        def_niandai = 2016
        def_dianti = '有'
        def_guapai = datetime.now().strftime('%Y-%m-%d')
        def_gengxin = datetime.now().strftime('%Y-%m-%d')
        def_leixing = '住宅'
        def_wuyefei = 2.0
        def_jianzhu = '塔楼'
        def_chanquan = '商品房'
        def_nianxian = 70
        def_lvhua = 35.00
        def_rongji = 3
        def_fenliu = '无'
        def_loudong = 1
        def_hushu = 1000

        city_name = item['chengshi']
        qu_name = item['qu']
        pian_name = item['pian']
        xiaoqu_name = item['xiaoqu']

        huxing_name = def_huxing_name
        p_huxing = r'\d+室\d+厅\d+卫'
        if(re.match(p_huxing,item['huxing'])):
            huxing_name = item['huxing']
        xuexiao_name = item['xuexiao']
        result['xiaoqu'] = self.get_or_insert_xiaoqu(city_name, qu_name, pian_name, xiaoqu_name)
        result['huxing'] = self.get_or_insert_huxing(huxing_name)

        result['mianji'] = def_mianji
        if(item['mianji'].find('平')>0 and self.isnumber(item['mianji'].split('平')[0])):
            result['mianji'] = float(item['mianji'].split('平')[0])
        else:
            self.logger.info('无效的面积 - {0}'.format(item['mianji']))

        result['danjia'] = def_danjia
        if(item['danjia'].find('元')>0 and self.isnumber(item['danjia'].split('元')[0])):
            result['danjia'] = float(item['danjia'].split('元')[0])
        else:
            self.logger.info('无效的单价 - {0}'.format(item['danjia']))

        result['chaoxiang'] = chaoxiang_choices[def_chaoxiang]
        if(item['chaoxiang'] in chaoxiang_choices):
            result['chaoxiang'] = chaoxiang_choices[item['chaoxiang']]

        result['louceng'] = louceng_choices[def_louceng]
        if(item['louceng'] in louceng_choices):
            result['louceng'] = louceng_choices[item['louceng']]

        result['zonglouceng'] = def_zonglouceng
        p_zonglouceng = r'.*共\d+层.*'
        if(re.match(p_zonglouceng,item['zonglouceng'])):
            result['zonglouceng'] = item['zonglouceng'].split('共')[1].split('层')[0]

        result['zhuangxiu'] = zhuangxiu_choices[def_zhuangxiu]
        if(item['zhuangxiu'] in zhuangxiu_choices):
            result['zhuangxiu'] = zhuangxiu_choices[item['zhuangxiu']]

        result['xuexiao'] = self.get_or_insert_xuexiao(city_name,xuexiao_name)

        result['niandai'] = def_niandai
        if(item['niandai'].find('年')>0 and item['niandai'].split('年')[0].isnumeric()):
            result['niandai'] = int(item['niandai'].split('年')[0])

        result['dianti'] = dianti_choices[def_dianti]
        if(item['dianti'] in dianti_choices):
            result['dianti'] = dianti_choices[item['dianti']]

        result['chanquan'] = chanquan_choices[def_chanquan]
        if(item['chanquan'] in chanquan_choices):
            result['chanquan'] = chanquan_choices[item['chanquan']]

        result['guapai'] = def_guapai
        p_guapai = r'20\d{2}-\d{2}-\d{2}'
        if(re.match(p_guapai,item['guapai'])):
            result['guapai'] = item['guapai']

        result['gengxin'] = def_gengxin
        if(item['gengxin'].find('天')>0):
            tian = int(item['gengxin'].split('天')[0])
            td = timedelta(days=tian)
            result['gengxin'] = (datetime.now() - td).strftime('%Y-%m-%d')

        result['leixing'] = leixing_choices[def_leixing]
        if(item['leixing'] in leixing_choices):
            result['leixing'] = leixing_choices[item['leixing']]

        result['wuyefei'] = def_wuyefei
        if(item['wuyefei'].find('元')>0 and item['wuyefei'].split('元')[0].isnumeric()):
            result['wuyefei'] = float(item['wuyefei'].split('元')[0])

        result['jianzhu'] = jianzhu_choices[def_jianzhu]

        if(item['jianzhu'] in jianzhu_choices):
            result['jianzhu'] = jianzhu_choices[item['jianzhu']]
        elif(item['jianzhu'].find(',')>0 and item['jianzhu'].split(',')[0] in jianzhu_choices):
            result['jianzhu'] = jianzhu_choices[item['jianzhu'].split(',')[0]]

        result['nianxian'] = def_nianxian
        if(item['nianxian'] != '' and item['nianxian'][:2].isnumeric()):
            result['nianxian'] = int(item['nianxian'][:2])

        result['lvhua'] = def_lvhua
        if(item['lvhua'].find('%')>0):
            result['lvhua'] = float(item['lvhua'].split('%')[0])

        result['rongji'] = def_rongji
        if(item['rongji'].isnumeric()):
            result['rongji'] = float(item['rongji'])

        result['fenliu'] = fenliu_choices[def_fenliu]
        if(item['fenliu'] in fenliu_choices):
            result['fenliu'] = fenliu_choices[item['fenliu']]

        result['loudong'] = def_loudong
        if(item['loudong'].isnumeric()):
            result['loudong'] = int(item['loudong'])

        result['hushu'] = def_hushu
        if(item['hushu'].isnumeric()):
            result['hushu'] = int(item['hushu'])

        result['zhiwen'] = item['zhiwen']
        return result

    def insert_fangwu(self,item):
        try:
            sql = "select * from house_fangwu where zhiwen='{0}';".format(item['zhiwen'])
            rows = self.readDb(sql)
            if(len(rows) > 0):
                self.logger.info("house_fangwu数据已存在")
                return
            item_fangwu = self.create_fangwu_data(item)

            str_keys = ("xiaoqu_id,huxing_id,mianji,danjia,chaoxiang,louceng,zonglouceng,zhuangxiu,"
                       "xuexiao_id,niandai,dianti,chanquan,guapai,gengxin,leixing,wuyefei,"
                       "jianzhu,nianxian,lvhua,rongji,fenliu,loudong,hushu,zhiwen")
            str_values = ("{0},{1},{2},{3},{4},{5},{6},{7},"
                         "{8},{9},{10},{11},'{12}','{13}',{14},{15},"
                         "{16},{17},{18},{19},{20},{21},{22},'{23}'"). \
                             format(item_fangwu['xiaoqu'],item_fangwu['huxing'],item_fangwu['mianji'],
                                    item_fangwu['danjia'],item_fangwu['chaoxiang'],item_fangwu['louceng'],
                                    item_fangwu['zonglouceng'],item_fangwu['zhuangxiu'],item_fangwu['xuexiao'],
                                    item_fangwu['niandai'],item_fangwu['dianti'],item_fangwu['chanquan'],
                                    item_fangwu['guapai'],item_fangwu['gengxin'],item_fangwu['leixing'],
                                    item_fangwu['wuyefei'],item_fangwu['jianzhu'],item_fangwu['nianxian'],
                                    item_fangwu['lvhua'],item_fangwu['rongji'],item_fangwu['fenliu'],
                                    item_fangwu['loudong'],item_fangwu['hushu'],item_fangwu['zhiwen'])
            sql = 'insert into house_fangwu ({0}) values ({1});'.format(str_keys,str_values)
            # self.cur.execute(sql)
            self.writeDb(sql)
        except Exception as e:
            self.logger.error("%s: %s" % (e.__class__.__name__, e))

    def insert_caiji(self,item):
        try:
            zhiwen = self.get_zhiwen(item)
            create_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            item['create_date'] = create_date
            item['zhiwen'] = zhiwen
            sql = "select * from house_caiji where zhiwen='{0}';".format(zhiwen)
            rows = self.readDb(sql)
            if len(rows) > 0:
                self.logger.info("house_caiji数据已存在")
                return
            self.insert(item,"house_caiji")
        except Exception as e:
            self.logger.error("%s: %s" % (e.__class__.__name__, e))

    def insert(self,item,table_name):
        keys = ''
        values = ''
        for key,value in item.items():
            keys += "{},".format(key)
            values += "'{}',".format(value)
        keys = keys.strip(',')
        values = values.strip(',')
        sql = "insert into {0} ({1}) values ({2})".format(table_name,keys,values)
        self.writeDb(sql)

    def get_zhiwen(self,item):
        str = item['chengshi'] + item['qu'] + item['pian'] + item['xiaoqu'] \
              + item['huxing'] + item['mianji'] + item['danjia'] + item['chaoxiang'] \
              + item['louceng'] + item['zonglouceng'] + item['zhuangxiu']
        zhiwen = hashlib.md5(str.encode('utf-8')).hexdigest()
        return zhiwen

    def hello(self):
        print ('hello,this is pg_helper')

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