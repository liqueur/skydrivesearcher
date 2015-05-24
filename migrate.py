#coding:utf-8

from __future__ import unicode_literals
from pymongo import MongoClient
from time import time
import hashlib

db = MongoClient().sds
start = time()

uk_list = [item['uk'] for item in db.user.find()]
db.new_user.update({'origin':'baiduyun'},
                   {'$addToSet':{'uk_list':{'$each':uk_list}}},
                   True)

print 'cost {} s'.format(time() - start)
