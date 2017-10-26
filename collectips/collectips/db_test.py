# -*- coding: utf-8 -*-

import psycopg2


def select(conn):
    cursor=conn.cursor()
    sql = "select * from xici"
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        print row[0]

def insert(conn):
    cursor=conn.cursor()
    sql = "insert into xici (ip,port,type,position,speed,last_check_time)" + \
    " values('%s','%s','%s','%s','%s','%s')"
    sql = sql % ("ip2","port","type","position",
            "speed","last_check_time")
    cursor.execute(sql)
    conn.commit()
    conn.close()
    # rows = cursor.fetchall()




conn = psycopg2.connect(database="scrapy", user="postgres",
                        password="123123", host="127.0.0.1", port="5432")
# select(conn)
insert(conn)