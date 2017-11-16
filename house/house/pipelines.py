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
from house.caiji_helper import Caiji_helper


class HousePipeline(object):


    def process_item(self, item, spider):
        if spider.name == "fang":
            caiji_helper = Caiji_helper()
            caiji_helper.insert_caiji(item)
            # pg_helper.insert_from_caiji(item)
        elif spider.name == "lianjia":
            caiji_helper = Caiji_helper()
            caiji_helper.insert_caiji(item)
        return item

