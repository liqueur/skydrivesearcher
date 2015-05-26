#coding:utf-8

import logging
from logging.handlers import TimedRotatingFileHandler

def gen_logger(name, filename, filemode):
    format = '[%(filename)s:%(lineno)d %(asctime)s] %(levelname)s %(message)s'
    logging.basicConfig(
        level=logging.DEBUG,
        filename=filename,
        filemode=filemode,
        format=format,
    )

    # rotate = TimedRotatingFileHandler(filename, when="h", interval=1, backupCount=7)
    # rotate.setFormatter(formatter)
    # logger.addHandler(rotate)

    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    formatter = logging.Formatter(format)
    console.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.addHandler(console)

    return logger
