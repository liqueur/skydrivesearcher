#coding:utf-8

import os
from pymongo import MongoClient
from indexing import index
from os.path import join, dirname, abspath
from settings import LOG_DIR

db = torndb.Connection('localhost:3306', 'sds', user='root', password='britten')

# 清空日志
for parent, dirnames, filenames in os.walk(LOG_DIR):
    for filename in filenames:
        filepath = join(parent, filename)
        os.remove(filepath)

# 初始化索引
index()
