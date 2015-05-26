#coding:utf-8

import lucene
import tornado
import tornado.web
import tornado.ioloop
import logging
from pymongo import MongoClient
from time import localtime, strftime, time
from lucene import *
from tools import gen_logger

lucene.initVM()
db = MongoClient().sds
indexdir = SimpleFSDirectory(File('index'))
searcher = IndexSearcher(indexdir)
# analyzer = StandardAnalyzer(Version.LUCENE_30)
# analyzer = ChineseAnalyzer(Version.LUCENE_30)
analyzer = CJKAnalyzer(Version.LUCENE_30)
formatter = SimpleHTMLFormatter("<span class=\'highlight\'>", "</span>")
logger = None
source_count = None

logger = gen_logger(__name__, 'log/serve.log', 'a')

def rebuild_indexing():
    '''
    :desc:重构索引
    '''
    logger.info('重构索引...')
    db = MongoClient().sds
    start_time = time()
    items = db.source.find()
    global source_count
    source_count = db.source.count()
    logger.info('收录数据 {} 条'.format(source_count))
    writer = IndexWriter(indexdir, analyzer, True, IndexWriter.MaxFieldLength.UNLIMITED)
    for item in items:
        doc = Document()
        doc.add(Field('title', item['title'], Field.Store.YES, Field.Index.ANALYZED))
        doc.add(Field('url', item['url'], Field.Store.YES, Field.Index.NOT_ANALYZED))
        doc.add(Field('time', str(item['time']), Field.Store.YES, Field.Index.NOT_ANALYZED))
        writer.addDocument(doc)

    writer.close()
    cost_time = '%.3f s' % (time() - start_time)
    logger.info('重构索引完毕，耗时 {}'.format(cost_time,))

class IndexHandler(tornado.web.RequestHandler):
    def get(self):
        kwargs = dict(
            source_count=source_count,
        )
        self.render('index.html', **kwargs)

class QueryHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        query_string = self.get_argument('query', '')
        if query_string.strip() == '':
            self.redirect('/')
            self.finish()
        else:
            query = QueryParser(Version.LUCENE_30, 'title', analyzer).parse(query_string)
            scorer = QueryScorer(query, 'title')
            highlighter = Highlighter(formatter, scorer)
            highlighter.setTextFragmenter(SimpleSpanFragmenter(scorer))
            start_time = time()
            total_hits = searcher.search(query, 1000)
            cost_time = '%.3f ms' % ((time() - start_time) * 1000,)
            items = []
            for hit in total_hits.scoreDocs:
                doc= searcher.doc(hit.doc)
                title = doc.get('title')
                stream = TokenSources.getAnyTokenStream(searcher.getIndexReader(), hit.doc, 'title', doc, analyzer)
                title = highlighter.getBestFragment(stream, title)
                url = doc.get('url')
                ctime = int(doc.get('time'))
                item = dict(
                    title=title,
                    url=url,
                    time=ctime,
                )
                items.append(item)

            kwargs = dict(
                query=query_string,
                items=items,
                localtime=localtime,
                strftime=strftime,
                cost_time=cost_time,
            )
            self.render('result.html', **kwargs)
            self.finish()

class ChartHandler(tornado.web.RequestHandler):
    def get(self):
        self.render('chart.html')

settings = dict(
    debug=True,
    template_path='template',
    static_path='static',
)

application = tornado.web.Application([
    (r'/', IndexHandler),
    (r'/query', QueryHandler),
], **settings)

if __name__ == '__main__':
    # rebuild_indexing()
    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()
