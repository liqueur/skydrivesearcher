#coding:utf-8

from __future__ import unicode_literals
from twisted.internet import defer, reactor
from twisted.web.client import getPage
from pymongo import MongoClient
from time import time
import json
import logging
import re
import hashlib

db = MongoClient().sds

logging.basicConfig(
    level=logging.DEBUG,
    filename='log/follow.log',
    format='%(asctime)s [%(filename)s:%(lineno)d] %(levelname)s %(message)s',
    filemode='w',
)

console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s [%(filename)s:%(lineno)d] %(levelname)s %(message)s')
console.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.addHandler(console)

URL = 'http://yun.baidu.com/pcloud/friend/getfollowlist?query_uk={uk}&limit=24&start={start}'

LIMIT = 10

start = time()

def done(resp):
    if resp is not None: logger.debug(resp)
    reactor.stop()

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

def fetch_follow(data, success, limit=None):
    def gen_defer(url):
        def callback(resp):
            fans = json.loads(resp)
            try:
                success[url] = [x['follow_uk'] for x in fans['follow_list']]
                db.user.update({'origin':'baiduyun'},
                               {'$addToSet':{'uk_list':{'$each':success[url]}}},
                               True)
                # uk = re.findall(r'query_uk=(\d+)', url)[0]
                # db.user.update({'origin':'baiduyun'}, {'$pull':{'uk_list':uk}})
                data.remove(url)
                logger.debug(success[url])
            except KeyError:
                success.pop(url)
                data.remove(url)

        def errback(err):
            logger.debug('[F] {} {}'.format(err, url))
            data.remove(url)
            data.append(url)

        d = getPage(url.encode('utf-8'), timeout=15)
        d.addCallbacks(callback, errback)

        return d

    def prepare(_success):
        _data = []
        for url, follow_list in _success.iteritems():
            _data = [URL.format(uk=uk, start=0).encode('utf-8') for uk in follow_list]

        _data = list(set(_data))
        _success = {x: None for x in _data}

        return _data, _success

    dl = defer.DeferredList([gen_defer(url) for url in data[:limit]])
    dl.addCallbacks(loop, callbackArgs=[fetch_follow, data, success, limit, fetch_total_count, prepare, LIMIT])

def fetch_total_count(data, success, limit=None):
    def gen_defer(url):
        def callback(resp):
            fans = json.loads(resp)
            try:
                success[url] = fans['total_count']
                data.remove(url)
                logger.debug(fans['total_count'])
            except KeyError:
                success.pop(url)
                data.remove(url)

        def errback(err):
            logger.debug('[F] {} {}'.format(err, url))
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

    dl = defer.DeferredList([gen_defer(url) for url in data[:limit]])
    dl.addCallbacks(loop, callbackArgs=[fetch_total_count, data, success, limit, fetch_follow, prepare, LIMIT])

data = [URL.format(uk=uk, start=0).encode('utf-8') for uk in
        db.user.find_one({'origin':'baiduyun'})['uk_list'][60:70]]
success = {x: {} for x in data}
fetch_total_count(data, success, limit=LIMIT)

reactor.run()
