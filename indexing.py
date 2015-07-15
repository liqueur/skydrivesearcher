#coding:utf-8

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
    items = db.resource.find()
    source_count = db.resource.count()
    logger.info('收录数据 {} 条'.format(source_count))
    writer = IndexWriter(INDEX_DIR, ANALYZER, True, IndexWriter.MaxFieldLength.UNLIMITED)
    counter = 0
    for item in items:
        doc = Document()
        doc.add(Field('title', item['title'], Field.Store.YES, Field.Index.ANALYZED))
        doc.add(Field('url', str(item['url']), Field.Store.YES, Field.Index.NOT_ANALYZED))
        doc.add(Field('time', str(item['ctime']), Field.Store.YES, Field.Index.NOT_ANALYZED))
        writer.addDocument(doc)
        counter += 1
        if counter % 10000 == 0:
            logger.info('计数 {} / {}'.format(counter, source_count))

    writer.close()
    cost_time = '%.3f s' % (time() - start_time)
    logger.info('重构索引完毕，耗时 {}'.format(cost_time,))

if __name__ == '__main__':
    index()
