#!/usr/bin/env python
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
    count = db.resource.count()
    origin = 'baiduyun'
    db.resource_log.insert({'ctime':ctime, 'count':count, 'origin':origin})

def fetch_share(urls, success, remain_try_times=3):
    '''
    :desc:采集用户分享资源
    :param urls:需要采集的url列表
    :param success:成功采集的数据
    :param remain_try_times:剩余尝试次数
    '''
    def gen_link(item):
        if len(item['shorturl']):
            return BD_SHORT_SHARE_URL.format(shorturl=item['shorturl'])
        else:
            return BD_SHARE_SHARE_URL.format(uk=re.findall(r'uk=(\d+)', url)[0],
                                             shareid=item['shareId'])

    def fetch(url):
        try:
            resp = requests.get(url, timeout=15)
            shared = json.loads(resp.text)
            result = [dict(origin='baiduyun',
                           url=gen_link(x),
                           title=x['typicalPath'].rsplit('/')[-1],
                           ctime=x['ctime']) for x in shared['list'] if gen_link(x) and x['typicalCategory']]
            logger.debug('fetch_share {}'.format(url))
            return Success(url, result)
        except Exception as e:
            logger.error('fetch_share {} {}'.format(e, url))
            return Failure(url)

    jobs = [gevent.spawn(fetch, url) for url in urls]
    gevent.joinall(jobs)
    urls = [job.value.url for job in jobs if isinstance(job.value, Failure)]
    success.extend([job.value for job in jobs if isinstance(job.value, Success)])
    logger.debug('fetch_share remain {}'.format(len(urls)))

    if (not remain_try_times) or (not urls):
        logger.debug('losted {}'.format(urls))
        # 采集失败url入库
        for url in urls:
            db.losted.insert({'url':url, 'handler':'share.fetch_share', 'ctime':int(time())})

        # 新采集分享资源入库
        for item in success:
            if item.value: db.resource.insert(item.value)
    else:
        # 递归采集直至url列表全部采集成功或耗尽尝试次数
        logger.debug('repeat fetch_share')
        fetch_share(urls, success, remain_try_times - 1)

def fetch_total_count(urls, success, remain_try_times=3):
    '''
    :desc:采集用户分享总数
    :param urls:需要采集的url列表
    :param success:成功采集的数据
    :param remain_try_times:剩余尝试次数
    '''
    def fetch(url):
        try:
            resp = requests.get(url, timeout=15)
            total_count = json.loads(resp.text)['total_count']
            logger.debug('fetch_total_count count {}'.format(total_count))
            return Success(url, total_count)
        except Exception as e:
            logger.error('fetch_total_count {} {}'.format(e, url))
            return Failure(url)

    jobs = [gevent.spawn(fetch, url) for url in urls]
    gevent.joinall(jobs)
    urls = [job.value.url for job in jobs if isinstance(job.value, Failure)]
    success.extend([job.value for job in jobs if isinstance(job.value, Success)])
    logger.debug('fetch_total_count failure {}'.format(urls))

    if (not remain_try_times) or (not urls):
        logger.debug('losted {}'.format(urls))
        for url in urls:
            db.losted.insert({'url':url, 'handler':'share.fetch_total_count', 'ctime':int(time())})

        urls = []
        for item in success:
            if item.value == 0: item.value = 1

            uk = re.findall(r'query_uk=(\d+)', item.url)[0]
            urls.extend([RECORD_URL.format(uk=uk, page=page).encode('utf-8')
                for page in range(1, item.value / 60 + int(item.value % 60 > 0) + 1)])

        return urls
    else:
        logger.debug('repeat fetch_total_count')
        return fetch_total_count(urls, success, remain_try_times - 1)

def run():
    logger.info('run share')
    share_offset = int(db.status.find_one({'origin':'baiduyun'}, {'share_offset':1})['share_offset'])
    uk_list = db.user.find_one({'origin':'baiduyun'})['uk_list'][share_offset:share_offset+SHARE_LIMIT]
    if not uk_list:
        logger.debug('DONE!')
    else:
        urls = [SHARE_URL.format(uk=uk).encode('utf-8') for uk in uk_list]
        try:
            resp = requests.get(urls[0])
            errno = json.loads(resp.text)['errno']
            if errno == 0:
                db.status.update({'origin':'baiduyun'}, {'$inc':{'share_offset':len(uk_list)}})
                urls = fetch_total_count(urls, [], 10)
                fetch_share(urls, [], 10)
                finish()
            elif errno < 0:
                logger.error('errno: {}'.format(errno))
            else:
                db.user.update({'origin':'baiduyun'}, {'$pull':{'uk_list':uk_list[share_offset]}})
                logger.debug('pull uk {}'.format(uk_list[share_offset]))
        except Exception, e:
            logger.error('{}'.format(e))

if __name__ == '__main__':
    run()
