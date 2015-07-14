#coding:utf-8

import gevent.monkey
gevent.monkey.patch_socket()
import gevent
import requests
from tools import gen_logger, Success, Failure
from IPython import embed

logger = gen_logger(__file__, 'w')

d = dict.fromkeys('abc', None)

def run():
    def fetch(i):
        # try:
        if i % 2 == 0: raise KeyError
        logger.debug('fetch {}'.format(i))
        return Success(i, i)
        # except Exception as e:
            # logger.error('fetch {}'.format(i))
            # return Failure(i)

    jobs = [gevent.spawn(fetch, i) for i in range(10)]
    gevent.joinall(jobs)
    embed()

run()
