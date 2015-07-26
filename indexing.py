#!/usr/bin/env python
#coding:utf-8

from __future__ import unicode_literals
from lucene import *
from settings import db, INDEX_DIR, ANALYZER
from tools import gen_logger, pagination
from time import time

logger = gen_logger(__file__, 'w')

def index():
    '''
    :desc:重构索引
    '''
    logger.info('重构索引...')
    start_time = time()
    resource_count = db.get('select count(*) as count from resource')['count']
    logger.info('收录数据 {} 条'.format(resource_count))
    sql = 'select * from resource limit 10000 offset %s'
    counter = 0
    writer = IndexWriter(INDEX_DIR, ANALYZER, True, IndexWriter.MaxFieldLength.UNLIMITED)
    for offset in range(0, resource_count, 10000):
        items = db.query(sql, offset)
        for item in items:
            doc = Document()
            doc.add(Field('title', item['title'], Field.Store.YES, Field.Index.ANALYZED))
            doc.add(Field('url', item['url'], Field.Store.YES, Field.Index.NOT_ANALYZED))
            doc.add(Field('feed_time', str(item['feed_time']), Field.Store.YES, Field.Index.NOT_ANALYZED))
            doc.add(Field('feed_username', item['feed_username'], Field.Store.YES, Field.Index.NOT_ANALYZED))
            doc.add(Field('feed_user_uk', str(item['feed_user_uk']), Field.Store.YES, Field.Index.NOT_ANALYZED))
            doc.add(Field('origin', item['origin'], Field.Store.YES, Field.Index.NOT_ANALYZED))
            doc.add(Field('size', str(item['size']), Field.Store.YES, Field.Index.NOT_ANALYZED))
            doc.add(Field('v_cnt', str(item['v_cnt']), Field.Store.YES, Field.Index.NOT_ANALYZED))
            doc.add(Field('d_cnt', str(item['d_cnt']), Field.Store.YES, Field.Index.NOT_ANALYZED))
            doc.add(Field('t_cnt', str(item['t_cnt']), Field.Store.YES, Field.Index.NOT_ANALYZED))
            writer.addDocument(doc)
            counter += 1
            if counter % 10000 == 0:
                logger.info('计数 {} / {}'.format(counter, resource_count))

    writer.close()
    cost_time = int(time() - start_time)
    logger.info('重构索引完毕，耗时 {}'.format(cost_time,))
    index_time = int(time())
    db.insert('insert into index_log (index_time, count, cost_time) values (%s, %s, %s)',
              index_time, resource_count, cost_time)

if __name__ == '__main__':
    index()
