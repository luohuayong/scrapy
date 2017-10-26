# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import psycopg2


class CollectipsPipeline(object):
    def process_item(self, item, spider):
        conn = psycopg2.connect(database="scrapy", user="postgres", password="123123", host="127.0.0.1", port="5432")
        cursor=conn.cursor()
        sql = ("insert into xici (ip,port,type,position,speed,last_check_time)"
               " values('%s','%s','%s','%s','%s','%s')")
        sql = sql % (item['ip'],item['port'],item['type'],item['position'],
                     item['speed'],item['last_check_time'])
        cursor.execute(sql)
        conn.commit()
        conn.close()
        return item
