#!/usr/bin/env python
#coding:utf-8

from __future__ import unicode_literals
from twisted.internet import defer, reactor
from twisted.web.client import getPage
from time import time
from functools import partial
from tools import gen_logger
from settings import *
import json
import logging
import re
import requests

logger = gen_logger(__file__, 'w')
start = None

def finish():
    '''
    :desc:任务结束日志记录
    '''
    _time = int(time())
    count = db.source.count()
    origin = 'baiduyun'
    db.source_log.insert({'ctime':_time, 'count':count, 'origin':origin})
    reactor.stop()

def loop(resp, data, success, failure, repeat, turn, prepare):
    '''
    :desc:任务循环
    :param resp:爬虫接收数据
    :param data:任务源数据
    :param success:任务处理成功后的结果列表
    :param failure:任务处理失败后的结果列表
    :param repeat:任务循环执行的函数
    :param turn:任务执行完毕后继续执行的函数
    :param prepare:执行turn函数之前对success数据进行预处理的函数
    '''
    total_success = len(success) - len(data)
    total_failure = len(data)
    logger.debug('total success: {}'.format(total_success))
    logger.debug('total failure: {}'.format(total_failure))
    if len(data) == 0:
        if (turn is None) or (prepare is None) or (total_success == 0):
            logger.debug('cost time: {}'.format(time() - start))
            finish()
        else:
            logger.debug('turn {}'.format(turn.func.__name__))
            _data, _success, _failure = prepare(success)
            turn(_data, _success, _failure)
    else:
        logger.debug('repeat {}'.format(repeat.func.__name__))
        repeat(data, success, failure)

def fetch_shared(data, success, failure=None, limit=None, failure_limit=3):
    '''
    :desc:抓取分享资源列表，生成DeferredList
    :param data:任务源数据
    :param success:任务处理成功后的结果列表
    :param failure:任务处理失败后的结果列表
    :param limit:每次执行任务数量的限制
    :param failure_limit:失败容忍次数
    '''
    if success is None: success = {x: None for x in data}
    if failure is None: failure = {}
    def gen_defer(url):
        def callback(resp):
            shared = json.loads(resp)
            def gen_link(x):
                if x['typicalCategory'] < 0:
                    return -1
                if len(x['shorturl']):
                    return BD_SHORT_SHARE_URL.format(shorturl=x['shorturl'])
                else:
                    return BD_SHARE_SHARE_URL.format(uk=re.findall(r'uk=(\d+)', url)[0],
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
    '''
    :desc:抓取分享资源总数，生成DeferredList
    :param data:任务源数据
    :param success:任务处理成功后的结果列表
    :param failure:任务处理失败后的结果列表
    :param limit:每次执行任务数量的限制
    :param failure_limit:失败容忍次数
    '''
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
            _data.extend([RECORD_URL.format(uk=uk, page=page).encode('utf-8')
                for page in range(1, total_count / 60 + int(total_count % 60 > 0) + 1)])

        _success = {x: None for x in _data}
        _failure = {}
        return _data, _success, _failure

    repeat = partial(fetch_total_count, limit=limit, failure_limit=failure_limit)
    turn = partial(fetch_shared, limit=limit, failure_limit=failure_limit)

    dl = defer.DeferredList([gen_defer(url) for url in data[:limit]])
    dl.addCallbacks(loop, callbackArgs=[data, success, failure, repeat, turn, prepare])

def run():
    global start
    start = time()
    logger.info('run share')
    user_count = len(db.user.find_one({'origin':'baiduyun'}, {'uk_list':1})['uk_list'])
    share_offset = int(db.status.find_one({'origin':'baiduyun'}, {'share_offset':1})['share_offset'])
    if share_offset + SHARE_LIMIT >= user_count:
        logger.debug('all done')
        return -1
    else:
        uk_list = db.user.find_one({'origin':'baiduyun'})['uk_list']
        data = [SHARE_URL.format(uk=uk).encode('utf-8') for uk in uk_list[share_offset:share_offset+SHARE_LIMIT]]
        try:
            resp = requests.get(data[0]).content
            errno = json.loads(resp)['errno']
            if errno == 0:
                db.status.update({'origin':'baiduyun'}, {'$inc':{'share_offset':SHARE_LIMIT}})
                fetch_total_count(data, limit=SHARE_LIMIT)
            elif errno < 0:
                logger.error('errno: {} {}'.format(errno, data[0]))
            else:
                db.user.update({'origin':'baiduyun'}, {'$pull':{'uk_list':uk_list[share_offset]}})
                logger.debug('pull uk {}'.format(uk_list[share_offset]))
                return -1
        except Exception, e:
            logger.error('{}'.format(e))
            return -1

if __name__ == '__main__':
    if run() is None: reactor.run()
