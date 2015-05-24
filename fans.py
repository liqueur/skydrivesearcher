#coding:utf-8

from __future__ import unicode_literals
from twisted.internet import defer, reactor
from twisted.web.client import getPage
from pymongo import MongoClient
from time import time
import json
import logging
import re

db = MongoClient().sds
db.user.remove()
db.user.insert({'name':'baidu', 'list':[]})

logging.basicConfig(
    level=logging.DEBUG,
    filename='fans.log',
    format='%(asctime)s [%(filename)s:%(lineno)d] %(levelname)s %(message)s',
    filemode='w',
)

console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(filename)s:%(lineno)d] %(levelname)s %(message)s')
console.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.addHandler(console)

URL = 'http://yun.baidu.com/pcloud/friend/getfanslist?query_uk={uk}&limit=24&start={start}'
db.user.update({'name':'baidu'}, {'name':'baidu', 'list':[]})

start = time()

def done(resp):
    if resp is not None: logger.debug(resp)
    reactor.stop()

def loop(resp, repeat, data, success, limit, turn, prepare, turn_limit):
    logger.debug('total success: {}'.format(len(success) - len(data)))
    logger.debug('total failure: {}'.format(len(data)))
    if len(data) == 0:
        if (turn is None) or (prepare is None) or (turn_limit is None):
            logger.debug('cost time: {}'.format(time() - start))
            reactor.stop()
        else:
            _data, _success = prepare(success)
            turn(_data, _success, limit=turn_limit)
    else:
        logger.debug('repeat')
        repeat(data, success, limit)

def fetch_fans(data, success, limit=None):
    def gen_defer(url):
        def callback(resp):
            fans = json.loads(resp)
            success[url] = [x['fans_uk'] for x in fans['fans_list']]
            new_list = db.user.find_one({'name':'baidu'})['list'] + success[url]
            db.user.update({'name':'baidu'}, {'name':'baidu', 'list':new_list})
            data.remove(url)
            logger.debug(success[url])

        def errback(err):
            logger.debug('[F] {}'.format(url))
            data.remove(url)
            data.append(url)

        d = getPage(url.encode('utf-8'), timeout=15)
        d.addCallbacks(callback, errback)

        return d

    dl = defer.DeferredList([gen_defer(url) for url in data[:limit]])
    dl.addCallbacks(loop, callbackArgs=[fetch_fans, data, success, limit, None, None, None])

def fetch_total_count(data, success, limit=None):
    def gen_defer(url):
        def callback(resp):
            fans = json.loads(resp)
            success[url] = fans['total_count']
            data.remove(url)
            logger.debug(fans['total_count'])

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
            if total_count > 0:
                uk = re.findall(r'query_uk=(\d+)', url)[0]
                _data.extend([URL.format(uk=uk, start=start)
                    for start in range(0, total_count, 24)])

        _success = {x: None for x in _data}
        return _data, _success

    dl = defer.DeferredList([gen_defer(url) for url in data])
    dl.addCallbacks(loop, callbackArgs=[fetch_total_count, data, success, limit, fetch_fans, prepare, 10])

# data = [URL.format(uk=1208824379, start=0).encode('utf-8')]
data = [URL.format(uk=856454119, start=0).encode('utf-8')]
logger.debug(data)
success = {x: {} for x in data}
fetch_total_count(data, success)
# fetch_total_count(data, success)
# fetch_total_count(data, success)

reactor.run()
