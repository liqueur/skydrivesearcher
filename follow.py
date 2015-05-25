#coding:utf-8

from __future__ import unicode_literals
from twisted.internet import defer, reactor
from twisted.web.client import getPage
from pymongo import MongoClient
from time import time
from functools import partial
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

def loop(resp, data, success, failure, repeat, turn=None, prepare=None):
    total_success = len(success) - len(data)
    total_failure = len(data)
    logger.debug('total success: {}'.format(total_success))
    logger.debug('total failure: {}'.format(total_failure))
    if len(data) == 0:
        if (turn is None) or (prepare is None) or (total_success == 0):
            logger.debug('cost time: {}'.format(time() - start))
            reactor.stop()
        else:
            logger.debug('turn {}'.format(turn.func.__name__))
            _data, _success, _failure = prepare(success)
            turn(_data, _success, _failure)
    else:
        logger.debug('repeat {}'.format(repeat.func.__name__))
        repeat(data, success, failure)

def fetch_follow(data, success=None, failure=None, limit=None, failure_limit=3):
    if success is None: success = {x: None for x in data}
    if failure is None: failure = {}
    def gen_defer(url):
        def callback(resp):
            fans = json.loads(resp)
            try:
                success[url] = [x['follow_uk'] for x in fans['follow_list']]
                db.user.update({'origin':'baiduyun'},
                               {'$addToSet':{'uk_list':{'$each':success[url]}}},
                               True)
                data.remove(url)
                logger.debug(success[url])
            except KeyError:
                success.pop(url)
                data.remove(url)

        def errback(err):
            logger.error('{} {}'.format(err, url))
            failure_num = failure.get(url, 0)
            if failure_num >= failure_limit:
                logger.error('exhaust try times: {}'.format(url))
                success.pop(url)
                data.remove(url)
            else:
                failure[url] = failure_num + 1
                data.remove(url)
                data.append(url)

        d = getPage(url.encode('utf-8'), timeout=15)
        d.addCallbacks(callback, errback)

        return d

    def prepare(_success):
        _data = []
        for url, follow_list in _success.iteritems():
            _data.extend([URL.format(uk=uk, start=0).encode('utf-8') for uk in follow_list])

        _data = list(set(_data))
        _success = {x: None for x in _data}
        _failure = {}

        return _data, _success, _failure

    repeat = partial(fetch_follow, limit=limit, failure_limit=failure_limit)
    turn = partial(fetch_total_count, limit=limit, failure_limit=failure_limit)

    dl = defer.DeferredList([gen_defer(url) for url in data[:limit]])
    dl.addCallbacks(loop, callbackArgs=[data, success, failure, repeat, turn, prepare])

def fetch_total_count(data, success=None, failure=None, limit=None, failure_limit=3):
    if success is None: success = {x: None for x in data}
    if failure is None: failure = {}
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

            # TODO
            uk = int(re.findall(r'query_uk=(\d+)', url)[0])
            db.user.update({'origin':'baiduyun'}, {'$pull':{'list':uk}})
            db.user.update({'origin':'baiduyun'}, {'$addToSet':{'checked_list':uk}}, True)

        def errback(err):
            logger.error('{} {}'.format(err, url))
            failure_num = failure.get(url, 0)
            if failure_num >= failure_limit:
                logger.error('exhaust try times: {}'.format(url))
                success.pop(url)
                data.remove(url)
            else:
                failure[url] = failure_num + 1
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
        _failure = {}
        return _data, _success, _failure

    repeat = partial(fetch_total_count, limit=limit, failure_limit=failure_limit)
    turn = partial(fetch_follow, limit=limit, failure_limit=failure_limit)

    dl = defer.DeferredList([gen_defer(url) for url in data[:limit]])
    dl.addCallbacks(loop, callbackArgs=[data, success, failure, repeat, turn, prepare])

def main():
    user_count = len(db.user.find_one({'origin':'baiduyun'}, {'uk_list':1})['uk_list'])
    follow_offset = int(db.status.find_one({'origin':'baiduyun'}, {'follow_offset':1})['follow_offset'])
    if follow_offset + LIMIT >= user_count:
        db.status.update({'origin':'baiduyun'}, {'$inc':{'follow_offset':user_count}})
        logger.debug('all done')
    else:
        db.status.update({'origin':'baiduyun'}, {'$inc':{'follow_offset':LIMIT}})
        data = [URL.format(uk=uk, start=0).encode('utf-8') for uk in
                db.user.find_one({'origin':'baiduyun'})['uk_list'][follow_offset:follow_offset+LIMIT]]
        fetch_total_count(data, limit=LIMIT)

        reactor.run()

if __name__ == '__main__':
    main()
