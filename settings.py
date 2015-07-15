#coding:utf-8

import lucene
from lucene import *
from pymongo import MongoClient
from os.path import join, abspath, dirname

initVM()

db = MongoClient().sds

FOLLOW_URL = 'http://yun.baidu.com/pcloud/friend/getfollowlist?query_uk={uk}&limit=24&start={start}'
FOLLOW_LIMIT = 20

BD_SHORT_SHARE_URL = 'http://yun.baidu.com/s/{shorturl}'
BD_SHARE_SHARE_URL = 'http://yun.baidu.com/share/link?uk={uk}&shareid={shareid}'

SHARE_URL = 'http://yun.baidu.com/pcloud/feed/getsharelist?auth_type=1&start=1&limit=60&query_uk={uk}'
RECORD_URL = 'http://yun.baidu.com/share/homerecord?uk={uk}&page={page}&pagelength=60'

SHARE_LIMIT = 200

LOG_LIMIT = 25

RESULT_PAGE_SIZE = 20
RESULT_MAX_NUM = 5000

TEST_CHINESE_CONTENT = '''
中华人民共和国
'''

# ANALYZER = StandardAnalyzer(Version.LUCENE_30)
# ANALYZER = ChineseAnalyzer(Version.LUCENE_30)
# ANALYZER = CJKAnalyzer(Version.LUCENE_30)
ANALYZER = SmartChineseAnalyzer(Version.LUCENE_30)

# TESTINDEXPATH = join(dirname(abspath(__file__)), 'testindex')
# TESTINDEXDIR = SimpleFSDirectory(File(TESTINDEXPATH))
# TESTSEARCHER = IndexSearcher(TESTINDEXDIR)

STATIC_PATH = join(dirname(abspath(__file__)), 'static')
TEMPLATE_PATH = join(dirname(abspath(__file__)), 'template')
INDEX_PATH = join(dirname(abspath(__file__)), 'index')

INDEX_DIR = SimpleFSDirectory(File(INDEX_PATH))

FORMATTER = SimpleHTMLFormatter("<span class=\'highlight\'>", "</span>")
