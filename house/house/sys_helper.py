#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by Leo on 2017/11/10

"""
代码说明：
"""

from pg_helper import Pg_helper
import logging

class Sys_helper(object):

    def __init__(self):
        self.pg_helper = Pg_helper()
        self.logger = logging.getLogger(name=__name__)
        self.sys_dict = self.pg_helper.select_all('house_sys')

    def setDanjia(self,value):
        item = {'name': 'danjia','value': value}
        self.pg_helper.update(item,'house_sys')

    def getDanjia(self):
        for item in self.sys_dict:
            if(item['name']=='danjia'):
                return item['value']


if __name__ == '__main__':
    pass