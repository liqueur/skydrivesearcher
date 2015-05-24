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

logging.basicConfig(
    level=logging.DEBUG,
    filename='share.log',
    format='%(asctime)s [%(filename)s:%(lineno)d] %(levelname)s %(message)s',
    filemode='w',
)

console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(filename)s:%(lineno)d] %(levelname)s %(message)s')
console.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.addHandler(console)

BD_SHORT_URL = 'http://yun.baidu.com/s/{shorturl}'
BD_SHARE_URL = 'http://yun.baidu.com/share/link?uk={uk}&shareid={shareid}'

URL = 'http://yun.baidu.com/pcloud/feed/getsharelist?auth_type=1&start=1&limit=60&query_uk={uk}'
URL2 = 'http://yun.baidu.com/share/homerecord?uk={uk}&page={page}&pagelength=60'

LIMIT = 500

start = time()

def loop(resp, repeat, data, success, limit, turn, prepare, turn_limit):
    logger.debug('total success: {}'.format(len(success) - len(data)))
    logger.debug('total failure: {}'.format(len(data)))
    if len(data) == 0:
        if (turn is None) or (prepare is None):
            logger.debug('cost time: {}'.format(time() - start))
            reactor.stop()
        else:
            _data, _success = prepare(success)
            turn(_data, _success, limit=turn_limit)
    else:
        logger.debug('repeat')
        repeat(data, success, limit=limit)

def done(resp):
    if resp is not None: logger.debug(resp)
    reactor.stop()

def fetch_shared(data, success, limit=None):
    def gen_defer(url):
        def callback(resp):
            shared = json.loads(resp)
            def gen_link(x):
                if x['typicalCategory'] < 0:
                    return -1
                if len(x['shorturl']):
                    return BD_SHORT_URL.format(shorturl=x['shorturl'])
                else:
                    return BD_SHARE_URL.format(uk=re.findall(r'uk=(\d+)', url)[0],
                                               shareid=x['shareId'])

            success[url] = [dict(origin='baiduyun',
                                 url=gen_link(x),
                                 title=x['typicalPath'].rsplit('/')[-1],
                                 time=x['ctime']) for x in shared['list'] if gen_link(x) > 0]
            uk = re.findall(r'query_uk=(\d+)', url)[0]
            db.source.insert(success[url])
            data.remove(url)

        def errback(err):
            logger.debug('[F] {} {}'.format(err, url))
            data.remove(url)
            data.append(url)

        d = getPage(url, timeout=15)
        d.addCallbacks(callback, errback)

        return d

    dl = defer.DeferredList([gen_defer(url) for url in data[:limit]])
    dl.addCallbacks(loop, callbackArgs=[fetch_shared, data, success, limit, None, None, None])

def fetch_total_count(data, success, limit=None):
    def gen_defer(url):
        def callback(resp):
            shared = json.loads(resp)
            try:
                logger.debug('total_count: {}'.format(shared['total_count']))
                success[url] = shared['total_count']
                data.remove(url)
            except KeyError:
                success.pop(url)
                data.remove(url)

        def errback(err):
            logger.debug('[F] {}'.format(url))
            data.remove(url)
            data.append(url)

        d = getPage(url, timeout=15)
        d.addCallbacks(callback, errback)

        return d

    def prepare(_success):
        _data = []
        for url, total_count in _success.iteritems():
            if total_count == 0:
                total_count = 1
            uk = re.findall(r'query_uk=(\d+)', url)[0]
            _data.extend([URL2.format(uk=uk, page=page).encode('utf-8')
                for page in range(1, total_count / 60 + int(total_count % 60 > 0) + 1)])

        _success = {x: None for x in _data}
        return _data, _success

    dl = defer.DeferredList([gen_defer(url) for url in data[:limit]])
    dl.addCallbacks(loop, callbackArgs=[fetch_total_count, data, success, limit, fetch_shared, prepare, LIMIT])

data = [URL.format(uk=uk).encode('utf-8') for uk in db.user.find_one({'origin':'baiduyun'})['uk_list']]
success = {x: {} for x in data}
fetch_total_count(data, success, limit=LIMIT)

reactor.run()
