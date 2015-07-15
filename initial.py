#coding:utf-8

import os
from pymongo import MongoClient
from indexing import index
from os.path import join, dirname, abspath

db = MongoClient().sds
LOG_DIR = join(dirname(abspath(__file__)), 'log')

# 初始化爬虫运行状态
db.status.update({}, {'origin':'baiduyun', 'follow_offset':0, 'share_offset':0}, True)
# 初始化用户ID列表
db.user.update({}, {'$set':{'uk_list':[1208824379]}})
# 清空网盘资源
db.resource.remove()
# 清空失败链接
db.losted.remove()

# 清空日志
db.resource_log.remove()
db.user_log.remove()
db.traffic_log.remove()
for parent, dirnames, filenames in os.walk(LOG_DIR):
    for filename in filenames:
        filepath = join(parent, filename)
        os.remove(filepath)

# 初始化索引
index()
