#coding:utf-8

from __future__ import unicode_literals
from twisted.internet import defer, reactor
from twisted.web.client import getPage
from time import time
from pymongo import MongoClient
from functools import partial
import json
import logging
import re

db = MongoClient().sds

logging.basicConfig(
    level=logging.DEBUG,
    filename='log/share.log',
    format='%(asctime)s [%(filename)s:%(lineno)d] %(levelname)s %(message)s',
    filemode='w',
)

console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s [%(filename)s:%(lineno)d] %(levelname)s %(message)s')
console.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.addHandler(console)

BD_SHORT_URL = 'http://yun.baidu.com/s/{shorturl}'
BD_SHARE_URL = 'http://yun.baidu.com/share/link?uk={uk}&shareid={shareid}'

URL = 'http://yun.baidu.com/pcloud/feed/getsharelist?auth_type=1&start=1&limit=60&query_uk={uk}'
URL2 = 'http://yun.baidu.com/share/homerecord?uk={uk}&page={page}&pagelength=60'

LIMIT = 200

start = time()

def loop(resp, data, success, failure, repeat, turn, prepare):
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

def fetch_shared(data, success, failure=None, limit=None, failure_limit=3):
    if success is None: success = {x: None for x in data}
    if failure is None: failure = {}
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

            try:
                success[url] = [dict(origin='baiduyun',
                                     url=gen_link(x),
                                     title=x['typicalPath'].rsplit('/')[-1],
                                     time=x['ctime']) for x in shared['list'] if gen_link(x) > 0]
            except KeyError, e:
                logger.error('KeyError {} {}'.format(e, url))
            if success[url] and len(success[url]):
                db.source.insert(success[url])
            uk = re.findall(r'uk=(\d+)', url)[0]
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

    repeat = partial(fetch_shared, limit=limit, failure_limit=failure_limit)

    dl = defer.DeferredList([gen_defer(url) for url in data[:limit]])
    dl.addCallbacks(loop, callbackArgs=[data, success, failure, repeat, None, None])

def fetch_total_count(data, success=None, failure=None, limit=None, failure_limit=3):
    if success is None: success = {x: None for x in data}
    if failure is None: failure = {}
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
            if total_count == 0:
                total_count = 1
            uk = re.findall(r'query_uk=(\d+)', url)[0]
            _data.extend([URL2.format(uk=uk, page=page).encode('utf-8')
                for page in range(1, total_count / 60 + int(total_count % 60 > 0) + 1)])

        _success = {x: None for x in _data}
        _failure = {}
        return _data, _success, _failure

    repeat = partial(fetch_total_count, limit=limit, failure_limit=failure_limit)
    turn = partial(fetch_shared, limit=limit, failure_limit=failure_limit)

    dl = defer.DeferredList([gen_defer(url) for url in data[:limit]])
    dl.addCallbacks(loop, callbackArgs=[data, success, failure, repeat, turn, prepare])

def main():
    user_count = len(db.user.find_one({'origin':'baiduyun'}, {'uk_list':1})['uk_list'])
    share_offset = int(db.status.find_one({'origin':'baiduyun'}, {'share_offset':1})['share_offset'])
    if share_offset + LIMIT >= user_count:
        db.status.update({'origin':'baiduyun'}, {'$inc':{'share_offset':user_count}})
        logger.debug('all done')
    else:
        db.status.update({'origin':'baiduyun'}, {'$inc':{'share_offset':LIMIT}})
        data = [URL.format(uk=uk).encode('utf-8') for uk in
                db.user.find_one({'origin':'baiduyun'})['uk_list'][share_offset:share_offset+LIMIT]]
        fetch_total_count(data, limit=LIMIT)

        reactor.run()

if __name__ == '__main__':
    main()
