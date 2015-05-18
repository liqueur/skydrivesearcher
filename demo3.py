#coding:utf-8

from __future__ import unicode_literals
from twisted.internet import defer, reactor
from twisted.web.client import getPage
from time import time
from pymongo import MongoClient
import json
import logging
import re

db = MongoClient().sds
db.source.remove()
db.source.insert({'name':'baidu', 'list':[]})

logging.basicConfig(
    level=logging.DEBUG,
    filename='demo3.log',
    format='%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)s %(message)s',
    filemode='w',
)

console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(filename)s] %(levelname)s %(message)s')
console.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.addHandler(console)

BD_SHORT_URL = 'http://yun.baidu.com/s/{shorturl}'
BD_SHARE_URL = 'http://yun.baidu.com/share/link?uk={uk}&shareid={shareid}'

url = 'http://yun.baidu.com/pcloud/feed/getsharelist?auth_type=1&start=1&limit=60&query_uk={uk}'
# url = url.format(uk=1208824379)
url = url.format(uk=856454119)
url2 = 'http://yun.baidu.com/share/homerecord?uk={uk}&page={page}&pagelength=60'

start = time()

def loop(resp, repeat, data, success, turn, prepare):
    logger.debug('total success: {}'.format(len(success) - len(data)))
    logger.debug('total failure: {}'.format(len(data)))
    if len(data) == 0:
        if (turn is None) or (prepare is None):
            logger.debug('cost time: {}'.format(time() - start))
            reactor.stop()
        else:
            _data, _success = prepare(success)
            turn(_data, _success)
    else:
        logger.debug('repeat')
        repeat(data, success)

def errback(err):
    logger.error(err)

def done(resp):
    if resp is not None: logger.debug(resp)
    reactor.stop()

def fetch_shared(data, success):
    def _fetch_shared(url):
        def _(resp):
            shared = json.loads(resp)
            def gen_link(x):
                if x['typicalCategory'] < 0:
                    return -1
                if len(x['shorturl']):
                    return BD_SHORT_URL.format(shorturl=x['shorturl'])
                else:
                    return BD_SHARE_URL.format(uk=re.findall(r'uk=(\d+)', url)[0],
                                               shareid=x['shareId'])

            success[url] = [dict(url=gen_link(x),
                                 title=x['typicalPath'].rsplit('/')[-1],
                                 time=x['ctime']) for x in shared['list'] if gen_link(x) > 0]
            new_list = db.source.find_one({'name':'baidu'})['list'] + success[url]
            db.source.update({'name':'baidu'}, {'name':'baidu', 'list':new_list})
            data.remove(url)

        d = getPage(url.encode('utf-8'), timeout=15)
        d.addCallbacks(_, errback)
        # d.addBoth(errback)

        return d

    dl = defer.DeferredList([_fetch_shared(url) for url in data])
    dl.addCallbacks(loop, callbackArgs=[fetch_shared, data, success, None, None])

def fetch_total_count(data, success):
    def _fetch_total_count(url):
        def _(resp):
            shared = json.loads(resp)
            logger.debug('total_count: {}'.format(shared['total_count']))
            success[url] = shared['total_count']
            data.remove(url)

        d = getPage(url, timeout=15)
        d.addCallbacks(_, errback)

        return d

    def prepare(_success):
        _data = []
        for url, total_count in _success.iteritems():
            if total_count == 0:
                total_count = 1
            uk = re.findall(r'query_uk=(\d+)', url)[0]
            _data.extend([url2.format(uk=uk, page=page)
                for page in range(1, total_count / 60 + int(total_count % 60 > 0) + 1)])

        _success = {x: None for x in _data}
        return _data, _success

    dl = defer.DeferredList([_fetch_total_count(url) for url in data])
    dl.addCallbacks(loop, callbackArgs=[fetch_total_count, data, success, fetch_shared, prepare])


data = [url.encode('utf-8')]
success = {x: {} for x in data}
fetch_total_count(data, success)

reactor.run()
