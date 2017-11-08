# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


import psycopg2
import time
import logging
import hashlib

from house.pg_helper import Pg_helper


class HousePipeline(object):
    # def __init__(self):
    #     self.databse = "mysite"
    #     self.user = "postgres"
    #     self.password = "123123"
    #     self.host = "127.0.0.1"
    #     self.port = "5432"
    #
    # def insert(self,item,table_name):
    #     keys = ''
    #     values = ''
    #     for key,value in item.items():
    #         keys += "{},".format(key)
    #         values += "'{}',".format(value)
    #     keys = keys.strip(',')
    #     values = values.strip(',')
    #     sql = "insert into {0} ({1}) values ({2})".format(table_name,keys,values)
    #
    #     try:
    #         conn = psycopg2.connect(database=self.databse, user=self.user,
    #                                 password=self.password,host=self.host,
    #                                 port=self.port)
    #         cursor = conn.cursor()
    #         cursor.execute(sql)
    #         conn.commit()
    #     except Exception as e:
    #         print(e)
    #     finally:
    #         cursor.close()
    #         conn.close()
    #
    # def get_zhiwen(self,item):
    #     str = item['chengshi'] + item['qu'] + item['pian'] + item['xiaoqu'] \
    #           + item['huxing'] + item['mianji'] + item['danjia'] + item['chaoxiang'] \
    #           + item['louceng'] + item['zonglouceng'] + item['zhuangxiu']
    #     zhiwen = hashlib.md5(str.encode('utf-8')).hexdigest()
    #     return zhiwen
    #
    # def zhiwen_is_exist(self,zhiwen):
    #     sql = "select * from house_caiji where zhiwen='{0}'".format(zhiwen)
    #     conn = psycopg2.connect(database=self.databse, user=self.user,
    #                             password=self.password,host=self.host,
    #                             port=self.port)
    #     cursor = conn.cursor()
    #     cursor.execute(sql)
    #     rows = cursor.fetchall()
    #     cursor.close()
    #     conn.close()
    #     if len(rows) > 0:
    #         return True
    #     else:
    #         return False

    def process_item(self, item, spider):
        if spider.name == "fang":
            pg_helper = Pg_helper()
            pg_helper.insert_caiji(item)
            pg_helper.insert_fangwu(item)
            # create_date = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
            # item['create_date'] = create_date
            # item['zhiwen'] = pg_helper.get_zhiwen(item)
            # if(pg_helper.caiji_data_is_exist(item['zhiwen']) == False):
            #     pg_helper.insert(item,"house_caiji")
            #     logging.info("写入数据库")
            # else:
            #     logging.info("重复数据")
        return item

