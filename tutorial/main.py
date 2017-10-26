#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by Leo on 2017/8/28

"""
代码说明：
"""

from scrapy.cmdline import execute
import sys
import os

# 打断点调试py文件
# sys.path.append('D:\PyCharm\py_scrapyjobbole')
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
print(os.path.dirname(os.path.abspath(__file__)))
# execute(['scrapy', 'crawl', 'quotes'])

execute(['scrapy', 'crawl', 'che'])

if __name__ == '__main__':
    pass