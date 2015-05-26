#coding:utf-8

from __future__ import unicode_literals
from twisted.internet import defer, reactor
from twisted.web.client import getPage
from pymongo import MongoClient
from time import time, sleep
from functools import partial
from tools import gen_logger
import json
import logging
import re
import requests

db = MongoClient().sds

logger = gen_logger(__name__, 'log/follow.log', 'a')

URL = 'http://yun.baidu.com/pcloud/friend/getfollowlist?query_uk={uk}&limit=24&start={start}'
LIMIT = 20
DELAY = 10 * 60
start = None

def finish():
    _time = int(time())
    user_count = len(db.user.find_one({'origin':'baiduyun'}, {'uk_list':1})['uk_list'])
    origin = 'baiduyun'
    db.user_log.insert({'ctime':_time, 'user_count':user_count, 'origin':origin})
    sleep(DELAY)
    run()

def loop(resp, data, success, failure, repeat, turn=None, prepare=None):
    total_success = len(success) - len(data)
    total_failure = len(data)
    logger.debug('total success: {}'.format(total_success))
    logger.debug('total failure: {}'.format(total_failure))
    if len(data) == 0:
        if (turn is None) or (prepare is None) or (total_success == 0):
            logger.debug('cost time: {}'.format(time() - start))
            # reactor.stop()
            finish()
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
            except KeyError, e:
                logger.error('{} {} {}'.format(e, url, resp))
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

    dl = defer.DeferredList([gen_defer(url) for url in data[:limit]])
    dl.addCallbacks(loop, callbackArgs=[data, success, failure, repeat, None, None])

def fetch_total_count(data, success=None, failure=None, limit=None, failure_limit=3):
    if success is None: success = {x: None for x in data}
    if failure is None: failure = {}
    def gen_defer(url):
        def callback(resp):
            fans = json.loads(resp)
            try:
                success[url] = fans['total_count']
                data.remove(url)
                logger.debug('total count: {}'.format(fans['total_count']))
            except KeyError, e:
                logger.error('{} {} {}'.format(e, url, resp))
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

def run():
    global start
    start = time()
    logger.info('run follow')
    user_count = len(db.user.find_one({'origin':'baiduyun'}, {'uk_list':1})['uk_list'])
    follow_offset = int(db.status.find_one({'origin':'baiduyun'}, {'follow_offset':1})['follow_offset'])
    if follow_offset + LIMIT >= user_count:
        db.status.update({'origin':'baiduyun'}, {'$set':{'follow_offset':user_count}})
        logger.debug('all done')
        reactor.stop()
    else:
        data = [URL.format(uk=uk, start=0).encode('utf-8') for uk in
                db.user.find_one({'origin':'baiduyun'})['uk_list'][follow_offset:follow_offset+LIMIT]]
        try:
            resp = requests.get(data[0]).content
            errno = json.loads(resp)['errno']
            if errno == 0:
                db.status.update({'origin':'baiduyun'}, {'$inc':{'follow_offset':LIMIT}})
                fetch_total_count(data, limit=LIMIT)
            else:
                logger.error('errno: {}'.format(errno))
                sleep(DELAY)
                run()
        except Exception, e:
            logger.error('{}'.format(e))
            sleep(DELAY)
            run()

if __name__ == '__main__':
    run()
    reactor.run()
