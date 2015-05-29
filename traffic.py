#!/usr/bin/env python
#coding:utf-8

from __future__ import unicode_literals
from settings import *
from time import time

_time = int(time())
count = db.monitor.find_one()['traffic']
db.traffic_log.insert({'ctime':_time, 'count':count})
