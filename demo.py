#coding:utf-8

from settings import *
from time import localtime, strftime

def gen_line(item):
    _ctime = strftime('%Y-%m-%d %H:%M', localtime(item['ctime']))
    count = item['count']
    return '{ctime},{count}\n'.format(ctime=_ctime, count=count)

with open('test.csv', 'w') as f:
    f.write('ctime,user_count\n')
    lines = [gen_line(item) for item in db.user_log.find()]
    f.writelines(lines)
    f.write('\n')

    f.write('ctime,source_count\n')
    lines = [gen_line(item) for item in db.source_log.find()]
    f.writelines(lines)
    f.write('\n')

    f.write('ctime,traffic_count\n')
    lines = [gen_line(item) for item in db.traffic_log.find()]
    f.writelines(lines)
