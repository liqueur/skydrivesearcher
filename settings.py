#coding:utf-8

import lucene
import torndb
import MySQLdb
from lucene import *
from os.path import join, abspath, dirname

initVM()

db = torndb.Connection('localhost:3306', 'sds', user='root', password='britten')
# db = MySQLdb.connect('localhost', 'root', 'britten', 'sds')

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
LOG_DIR = join(dirname(abspath(__file__)), 'log')

FORMATTER = SimpleHTMLFormatter("<span class=\'highlight\'>", "</span>")
