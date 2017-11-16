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

    def insert(self,item,table_name):
        try:
            keys = ''
            values = ''
            for key,value in item.items():
                keys += "{},".format(key)
                values += "%({})s,".format(key)
            keys = keys.strip(',')
            values = values.strip(',')
            sql = "insert into {0} ({1}) values ({2})".format(table_name,keys,values)
            self.writeDb(sql,item)
        except Exception as e:
            self.logger.error("%s: %s" % (e.__class__.__name__, e))

    def select_all(self,table_name):
        try:
            sql = "select * from {0}".format(table_name)
            rows = self.readDb(sql)
            return rows
        except Exception as e:
            self.logger.error("%s: %s" % (e.__class__.__name__, e))

    def update(self,item,table_name):
        try:
            sets = ''
            for key,value in item.items():
                sets += "{0}=%{0}s,".format(key)
            sets = sets.strip(',')
            sql = "update {0} set {1} where id=%{id}s".format(table_name,sets)
            self.writeDb(sql,item)
        except Exception as e:
            self.logger.error("%s: %s" % (e.__class__.__name__, e))








    def hello(self):
        print ('hello,this is pg_helper')

if __name__ == '__main__':
    pass