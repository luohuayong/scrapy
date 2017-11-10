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

    def get_city(self,city_name):
        sql = "select id from house_city where name='{0}';".format(city_name)
        rows = self.readDb(sql)
        if len(rows) == 0:
            raise Exception('城市名称不存在 - {0}'.format(city_name))
        city_id = rows[0]['id']
        return city_id

    def get_or_insert_qu(self, city_name, qu_name):
        city_id = self.get_city(city_name)

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

    def get_chaoxiang(self,chaoxiang_name):
        sql = "select id from house_chaoxiang where name='{0}';".format(chaoxiang_name)
        rows = self.readDb(sql)
        if len(rows) == 0:
            raise Exception('朝向错误 - {0}'.format(chaoxiang_name))
        return rows[0]['id']

    def get_louceng(self,louceng,zhonglouceng):
        sql = "select id from house_louceng where lou='{0}' and max>={1} and min<{1};".\
            format(louceng,zhonglouceng)
        rows = self.readDb(sql)
        if len(rows) == 0:
            raise Exception('楼层错误 - {0}／{1}'.format(louceng,zhonglouceng))
        return rows[0]['id']

    def get_chanquan(self,chanquan_name):
        sql = "select id from house_chanquan where name='{0}年';".format(chanquan_name)
        rows = self.readDb(sql)
        if len(rows) == 0:
            raise Exception('产权错误 - {0}'.format(chanquan_name))
        return rows[0]['id']

    def get_jianzhu(self,jianzhu_name):
        sql = "select id from house_jianzhu where name='{0}';".format(jianzhu_name)
        rows = self.readDb(sql)
        if len(rows) == 0:
            raise Exception('建筑类型错误 - {0}'.format(jianzhu_name))
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

        result['city_id'] = self.get_city(city_name)
        result['qu_id'] = self.get_or_insert_qu(city_name,qu_name)
        result['pian_id'] = self.get_or_insert_pian(city_name,qu_name,pian_name)
        result['xiaoqu_id'] = self.get_or_insert_xiaoqu(city_name, qu_name, pian_name, xiaoqu_name)

        p_huxing = r'\d+室\d+厅\d+卫'
        if(re.match(p_huxing,item['huxing'])):
            result['huxing_id'] = self.get_or_insert_huxing(item['huxing'])
        else:
            raise Exception('无效的户型 - {0}'.format(item['huxing']))

        if(item['mianji'].find('平')>0 and self.isnumber(item['mianji'].split('平')[0])):
            result['mianji'] = float(item['mianji'].split('平')[0])
        else:
            raise Exception('无效的面积 - {0}'.format(item['mianji']))

        if(item['danjia'].find('元')>0 and self.isnumber(item['danjia'].split('元')[0])):
            result['danjia'] = float(item['danjia'].split('元')[0])
        else:
            raise Exception('无效的单价 - {0}'.format(item['danjia']))

        result['chaoxiang_id'] = self.get_chaoxiang(item['chaoxiang'])
        zonglouceng = item['zonglouceng'].split('共')[1].split('层')[0]
        result['louceng_id'] = self.get_louceng(item['louceng'],zonglouceng)

        # result['niandai'] = def_niandai
        if(item['niandai'].find('年')>0 and item['niandai'].split('年')[0].isnumeric()):
            result['niandai'] = int(item['niandai'].split('年')[0])
        else:
            raise Exception('无效的年代 - {0}'.format(item['niandai']))

        if(item['nianxian'] != '' and item['nianxian'][:2].isnumeric()):
            nianxian = int(item['nianxian'][:2])
            result['chanquan_id'] = self.get_chanquan(nianxian)
        else:
            raise Exception('无效的产权年限 - {0}'.format(item['nianxian']))

        p_guapai = r'20\d{2}-\d{2}-\d{2}'
        if(re.match(p_guapai,item['guapai'])):
            result['guapai'] = item['guapai']
        else:
            raise Exception('无效的挂牌日期 - {0}'.format(item['guapai']))

        # result['gengxin'] = def_gengxin
        if(item['gengxin'].find('天')>0):
            tian = int(item['gengxin'].split('天')[0])
            td = timedelta(days=tian)
            result['gengxin'] = (datetime.now() - td).strftime('%Y-%m-%d')
        else:
            result['gengxin'] = datetime.now().strftime('%Y-%m-%d')

        result['jianzhu_id'] = self.get_jianzhu(item['jianzhu'])

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

            str_keys = ("city_id,qu_id,pian_id,xiaoqu_id,huxing_id,mianji,danjia,chaoxiang_id,"
                        "louceng_id,niandai,chanquan_id,guapai,gengxin,jianzhu_id,zhiwen")
            str_values = ("{0},{1},{2},{3},{4},{5},{6},{7},"
                         "{8},{9},{10},'{11}','{12}',{13},'{14}'"). \
                             format(item_fangwu['city_id'],item_fangwu['qu_id'],item_fangwu['pian_id'],
                                    item_fangwu['xiaoqu_id'],item_fangwu['huxing_id'],item_fangwu['mianji'],
                                    item_fangwu['danjia'],item_fangwu['chaoxiang_id'],
                                    item_fangwu['louceng_id'],item_fangwu['niandai'],item_fangwu['chanquan_id'],
                                    item_fangwu['guapai'],item_fangwu['gengxin'],item_fangwu['jianzhu_id'],
                                    item_fangwu['zhiwen'])
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
            item['status'] = '0'
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

    def set_xiaoqu_danjia(self):
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