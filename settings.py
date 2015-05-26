#coding:utf-8

from pymongo import MongoClient

db = MongoClient().sds

FOLLOW_URL = 'http://yun.baidu.com/pcloud/friend/getfollowlist?query_uk={uk}&limit=24&start={start}'
FOLLOW_LIMIT = 20
FOLLOW_DELAY = 10 * 60

BD_SHORT_SHARE_URL = 'http://yun.baidu.com/s/{shorturl}'
BD_SHARE_SHARE_URL = 'http://yun.baidu.com/share/link?uk={uk}&shareid={shareid}'

SHARE_URL = 'http://yun.baidu.com/pcloud/feed/getsharelist?auth_type=1&start=1&limit=60&query_uk={uk}'
RECORD_URL = 'http://yun.baidu.com/share/homerecord?uk={uk}&page={page}&pagelength=60'

SHARE_LIMIT = 200
SHARE_DELAY = 2 * 60
