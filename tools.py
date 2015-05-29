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
