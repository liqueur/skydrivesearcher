#coding:utf-8

import gevent.monkey
gevent.monkey.patch_all()
import gevent
import requests
import json
import re
from time import time
from tools import gen_logger, Success, Failure
from settings import *

logger = gen_logger(__file__, 'w')

def finish():
    '''
    :desc:任务结束日志记录
    '''
    ctime = int(time())
    count = len(db.user.find_one({'origin':'baiduyun'}, {'uk_list':1})['uk_list'])
    origin = 'baiduyun'
    db.user_log.insert({'ctime':ctime, 'count':count, 'origin':origin})

def fetch_follow(urls, success, remain_try_times=3):
    '''
    :desc:采集订阅用户
    :param urls:需要采集的url列表
    :param success:成功采集的数据
    :param remain_try_times:剩余尝试次数
    '''
    def fetch(url):
        try:
            resp = requests.get(url, timeout=15)
            fans = json.loads(resp.text)
            follow_list = [x['follow_uk'] for x in fans['follow_list']]
            logger.debug('fetch_follow {}'.format(url))
            return Success(url, follow_list)
        except KeyError as e:
            logger.error('fetch_follow {} {}'.format(e, url))
            return Failure(url)

    jobs = [gevent.spawn(fetch, url) for url in urls]
    gevent.joinall(jobs)
    urls = [job.value.url for job in jobs if isinstance(job.value, Failure)]
    success.extend([job.value for job in jobs if isinstance(job.value, Success)])
    logger.debug('fetch_follow remain {}'.format(len(urls)))

    for j in jobs:
        logger.error(type(j.exception))

    if (not remain_try_times) or (not urls):
        logger.debug('losted {}'.format(urls))
        # 采集失败url入库
        for url in urls:
            db.losted.insert({'url':url, 'handler':'follow.fetch_follow', 'ctime':int(time())})

        # 新采集用户id入库
        for item in success:
            db.user.update({'origin':'baiduyun'},
                           {'$addToSet':{'uk_list':{'$each':item.value}}},
                           True)
    else:
        # 递归采集直至url列表全部采集成功或耗尽尝试次数
        fetch_follow(urls, success, remain_try_times - 1)

def fetch_total_count(urls, success, remain_try_times=3):
    '''
    :desc:采集用户订阅总数
    :param urls:需要采集的url列表
    :param success:成功采集的数据
    :param remain_try_times:剩余尝试次数
    '''
    def fetch(url):
        try:
            resp = requests.get(url, timeout=15)
            total_count = json.loads(resp.text)['total_count']
            # logger.debug('fetch_total_count count {}'.format(total_count))
            return Success(url, total_count)
        except Exception, e:
            logger.error('fetch_total_count {} {}'.format(e, url))
            return Failure(url)

    jobs = [gevent.spawn(fetch, url) for url in urls]
    gevent.joinall(jobs)


    urls = [job.value.url for job in jobs if isinstance(job.value, Failure)]
    success.extend([job.value for job in jobs if isinstance(job.value, Success)])
    logger.debug('fetch_total_count remain {}'.format(len(urls)))

    if (not remain_try_times) or (not urls):
        logger.debug('losted {}'.format(urls))
        for url in urls:
            db.losted.insert({'url':url, 'handler':'follow.fetch_total_count', 'ctime':int(time())})

        urls = []
        for item in success:
            if item.value > 0:
                uk = re.findall(r'query_uk=(\d+)', item.url)[0]
                urls.extend([FOLLOW_URL.format(uk=uk, start=start)
                    for start in range(0, item.value, 24)])
        return urls
    else:
        return fetch_total_count(urls, success, remain_try_times - 1)

def run():
    logger.info('run follow')
    user_count = len(db.user.find_one({'origin':'baiduyun'}, {'uk_list':1})['uk_list'])
    follow_offset = int(db.status.find_one({'origin':'baiduyun'}, {'follow_offset':1})['follow_offset'])
    if follow_offset + FOLLOW_LIMIT >= user_count:
        logger.debug('all done')
    else:
        urls = [FOLLOW_URL.format(uk=uk, start=0).encode('utf-8') for uk in
                db.user.find_one({'origin':'baiduyun'})['uk_list'][follow_offset:follow_offset+FOLLOW_LIMIT]]
        try:
            resp = requests.get(urls[0])
            errno = json.loads(resp.text)['errno']
            if errno == 0:
                db.status.update({'origin':'baiduyun'}, {'$inc':{'follow_offset':FOLLOW_LIMIT}})
                urls = fetch_total_count(urls, [], 10)
                fetch_follow(urls, [], 10)
                finish()
            else:
                logger.error('errno: {}'.format(errno))
        except Exception, e:
            logger.error('{}'.format(e))

if __name__ == '__main__':
    run()
