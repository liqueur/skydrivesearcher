#coding:utf-8

import logging
from logging.handlers import TimedRotatingFileHandler
from os.path import join, dirname, abspath

log_dir = join(dirname(abspath(__file__)), 'log')

def gen_logger(filename, filemode):
    format = '[%(filename)s:%(lineno)d %(asctime)s] %(levelname)s %(message)s'
    filename = filename.rsplit('.', 1)[0]
    filename = '.'.join([filename, 'log'])
    filename = join(log_dir, filename)
    logging.basicConfig(
        level=logging.DEBUG,
        filename=filename,
        filemode=filemode,
        format=format,
    )

    # rotate = TimedRotatingFileHandler(filename, when="h", interval=1, backupCount=7)
    # rotate.setFormatter(formatter)
    # logger.addHandler(rotate)

    formatter = logging.Formatter(format)
    logger = logging.getLogger(filename)
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    console.setFormatter(formatter)
    logger.addHandler(console)

    return logger

def pagination(seq, page, page_size):
    r1, r2 = divmod(len(seq), page_size)
    page_num = r1 + int(r2 > 0)
    if page > page_num:
        page = 1

    objects = seq[(page - 1) * page_size:page * page_size]
    previous_page = page - 1 if page > 1 else None
    next_page = page + 1 if page < page_num else None
    is_empty = len(objects) == 0

    return dict(objects=objects,
                page=page,
                page_num=page_num,
                previous_page=previous_page,
                next_page=next_page,
                is_empty=is_empty)

class Success(object):
    def __init__(self, url, value):
        self.url = url
        self.value = value

class Failure(object):
    def __init__(self, url):
        self.url = url
