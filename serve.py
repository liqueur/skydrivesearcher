#coding:utf-8

import lucene
import tornado
import tornado.web
import tornado.ioloop
import logging
import string
import json
from pymongo import MongoClient
from time import localtime, strftime, time
from lucene import *
from settings import *
from tools import gen_logger
from functools import wraps
from StringIO import StringIO

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

logger = gen_logger(__file__, 'w')

def traffic_counter(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        db.monitor.update({}, {'$inc':{'traffic':1}})
        return func(*args, **kwargs)
    return wrapper

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
    @traffic_counter
    def get(self):
        kwargs = dict(
            source_count=source_count,
        )
        self.render('index.html', **kwargs)

class QueryHandler(tornado.web.RequestHandler):
    @traffic_counter
    @tornado.web.asynchronous
    def get(self):
        query_string = self.get_argument('query', '').strip()
        if query_string == '':
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

class OverlookHandler(tornado.web.RequestHandler):
    def get(self):
        self.render('dashboard.html')

class UserLogHandler(tornado.web.RequestHandler):
    def get(self):
        user_log_list = list(db.user_log.find())
        def gen_time(ctime):
            return strftime('%H:%M', localtime(ctime))

        if len(user_log_list) <= LOG_LIMIT:
            ctime_list = [gen_time(x['ctime']) for x in user_log_list]
            count_list = [x['count'] for x in user_log_list]
        else:
            user_log_list = user_log_list[len(user_log_list) - LOG_LIMIT:]
            ctime_list = [gen_time(x['ctime']) for x in user_log_list]
            count_list = [x['count'] for x in user_log_list]

        data = dict(
            ctime_list=ctime_list,
            count_list=count_list,
        )

        self.write(json.dumps(data))

class SourceLogHandler(tornado.web.RequestHandler):
    def get(self):
        source_log_list = list(db.source_log.find())
        def gen_time(ctime):
            return strftime('%H:%M', localtime(ctime))

        if len(source_log_list) <= LOG_LIMIT:
            ctime_list = [gen_time(x['ctime']) for x in source_log_list]
            count_list = [x['count'] for x in source_log_list]
        else:
            source_log_list = source_log_list[len(source_log_list) - LOG_LIMIT:]
            ctime_list = [gen_time(x['ctime']) for x in source_log_list]
            count_list = [x['count'] for x in source_log_list]

        data = dict(
            ctime_list=ctime_list,
            count_list=count_list,
        )

        self.write(json.dumps(data))

class TrafficLogHandler(tornado.web.RequestHandler):
    def get(self):
        traffic_log_list = list(db.traffic_log.find())
        def gen_time(ctime):
            return strftime('%H:%M', localtime(ctime))

        if len(traffic_log_list) <= LOG_LIMIT:
            ctime_list = [gen_time(x['ctime']) for x in traffic_log_list]
            count_list = [x['count'] for x in traffic_log_list]
        else:
            traffic_log_list = traffic_log_list[len(traffic_log_list) - LOG_LIMIT:]
            ctime_list = [gen_time(x['ctime']) for x in traffic_log_list]
            count_list = [x['count'] for x in traffic_log_list]

        data = dict(
            ctime_list=ctime_list,
            count_list=count_list,
        )

        self.write(json.dumps(data))

class UserLogCSVHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        filename = 'user.log.{}.csv'.format(int(time()))
        self.set_header('Content-Type', 'application/octet-stream')
        self.set_header('Content-Disposition', 'attachment;filename='+filename)

        def gen_line(item):
            _ctime = strftime('%Y-%m-%d %H:%M', localtime(item['ctime']))
            count = item['count']
            return '{ctime},{count}\n'.format(ctime=_ctime, count=count)

        def yield_line():
            for item in db.user_log.find():
                yield gen_line(item)

        self.write('ctime,user_count\n')
        for line in yield_line():
            self.write(line)

        self.finish()

class SourceLogCSVHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        filename = 'source.log.{}.csv'.format(int(time()))
        self.set_header('Content-Type', 'application/octet-stream')
        self.set_header('Content-Disposition', 'attachment;filename='+filename)

        def gen_line(item):
            _ctime = strftime('%Y-%m-%d %H:%M', localtime(item['ctime']))
            count = item['count']
            return '{ctime},{count}\n'.format(ctime=_ctime, count=count)

        def yield_line():
            for item in db.source_log.find():
                yield gen_line(item)

        self.write('ctime,source_count\n')
        for line in yield_line():
            self.write(line)

        self.finish()

class TrafficLogCSVHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        filename = 'traffic.log.{}.csv'.format(int(time()))
        self.set_header('Content-Type', 'application/octet-stream')
        self.set_header('Content-Disposition', 'attachment;filename='+filename)

        def gen_line(item):
            _ctime = strftime('%Y-%m-%d %H:%M', localtime(item['ctime']))
            count = item['count']
            return '{ctime},{count}\n'.format(ctime=_ctime, count=count)

        def yield_line():
            for item in db.traffic_log.find():
                yield gen_line(item)

        self.write('ctime,traffic_count\n')
        for line in yield_line():
            self.write(line)

        self.finish()

class MessageHandler(tornado.web.RequestHandler):
    def get(self):
        return self.render('message.html')

class NoticeHandler(tornado.web.RequestHandler):
    def get(self):
        return self.render('notice.html')

class DonateHandler(tornado.web.RequestHandler):
    def get(self):
        return self.render('donate.html')

settings = dict(
    debug=True,
    template_path='template',
    static_path='static',
)

application = tornado.web.Application([
    (r'/', IndexHandler),
    (r'/query', QueryHandler),
    (r'/admin', OverlookHandler),
    (r'/admin/overlook', OverlookHandler),
    (r'/admin/userlog', UserLogHandler),
    (r'/admin/userlog/csv', UserLogCSVHandler),
    (r'/admin/sourcelog', SourceLogHandler),
    (r'/admin/sourcelog/csv', SourceLogCSVHandler),
    (r'/admin/trafficlog', TrafficLogHandler),
    (r'/admin/trafficlog/csv', TrafficLogCSVHandler),
    (r'/admin/message', MessageHandler),
    (r'/admin/notice', NoticeHandler),
    (r'/admin/donate', DonateHandler),
], **settings)

if __name__ == '__main__':
    # rebuild_indexing()
    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()
