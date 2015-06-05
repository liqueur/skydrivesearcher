#coding:utf-8

import lucene
import tornado
import tornado.web
import tornado.ioloop
import logging
import string
import json
import re
from pymongo import MongoClient
from time import localtime, strftime, time
from lucene import *
from settings import *
from tools import gen_logger, pagination
from functools import wraps
from IPython import embed

testindexdir = SimpleFSDirectory(File('testindex'))
testsearcher = IndexSearcher(testindexdir)
source_count = None
logger = gen_logger(__file__, 'w')

def traffic_counter(func):
    '''
    :desc:流量统计
    '''
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
    start_time = time()
    items = db.source.find()
    global source_count
    source_count = db.source.count()
    logger.info('收录数据 {} 条'.format(source_count))
    writer = IndexWriter(INDEXDIR, ANALYZER, True, IndexWriter.MaxFieldLength.UNLIMITED)
    counter = 0
    for item in items:
        doc = Document()
        doc.add(Field('title', item['title'], Field.Store.YES, Field.Index.ANALYZED))
        doc.add(Field('url', item['url'], Field.Store.YES, Field.Index.NOT_ANALYZED))
        doc.add(Field('time', str(item['time']), Field.Store.YES, Field.Index.NOT_ANALYZED))
        writer.addDocument(doc)
        counter += 1
        if counter % 10000 == 0:
            logger.info('计数 {} / {}'.format(counter, source_count))

    writer.close()
    cost_time = '%.3f s' % (time() - start_time)
    logger.info('重构索引完毕，耗时 {}'.format(cost_time,))

class IndexHandler(tornado.web.RequestHandler):
    '''
    :desc:首页
    '''
    @traffic_counter
    def get(self):
        self.render('index.html')

class SearchHandler(tornado.web.RequestHandler):
    '''
    :desc:搜索
    '''
    @traffic_counter
    @tornado.web.asynchronous
    def get(self):
        # 转义字符前加上\
        def replace(matched):
            return '\{escape}'.format(escape=matched.group('escape'))
        query_string = self.get_argument('query_string', '').strip()
        page = int(self.get_argument('page', 1))
        if query_string == '':
            self.send_error(400)
        else:
            # 转义特殊字符
            query_string = re.sub(r'(?P<escape>[-+!\\():^\]\[{}~*?])', replace, query_string)
            # 解析用户查询
            query = QueryParser(Version.LUCENE_30, 'title', ANALYZER).parse(query_string)
            scorer = QueryScorer(query, 'title')
            # 设置高亮器
            highlighter = Highlighter(FORMATTER, scorer)
            highlighter.setTextFragmenter(SimpleSpanFragmenter(scorer))
            start_time = time()
            # 开始搜索
            total_hits = SEARCHER.search(query, RESULT_MAX_NUM)
            cost_time = '%.3f 秒' % ((time() - start_time),)
            # 符合条件的资源数量
            total_count = len(total_hits.scoreDocs)

            t1 = time()

            # 对搜索结果进行高亮和封装
            # 对搜索结果分页
            paging = pagination(total_hits.scoreDocs, page, RESULT_PAGE_SIZE)

            def wrap(hit):
                doc= SEARCHER.doc(hit.doc)
                title = doc.get('title')
                stream = TokenSources.getAnyTokenStream(SEARCHER.getIndexReader(), hit.doc, 'title', doc, ANALYZER)
                title = highlighter.getBestFragment(stream, title)
                url = doc.get('url')
                ctime = int(doc.get('time'))
                item = dict(
                    title=title,
                    url=url,
                    time=strftime('%Y-%m-%d %H:%M:%S', localtime(ctime))
                )
                return item

            paging['objects'] = map(wrap, paging['objects'])

            data_time = '%.3f 秒' % ((time() - t1),)

            kwargs = dict(
                cost_time=cost_time,
                data_time=data_time,
                paging=paging,
                total_count=total_count,
            )

            self.write(json.dumps(kwargs))
            self.finish()

class OverlookHandler(tornado.web.RequestHandler):
    def get(self):
        self.render('dashboard.html')

class OverlookChartHandler(tornado.web.RequestHandler):
    def get(self):
        chartname = self.get_argument('chartname', None)
        log = {
            'user': list(db.user_log.find()),
            'source': list(db.source_log.find()),
            'traffic': list(db.traffic_log.find()),
        }.get(chartname, None)
        if log is None: self.send_error(400)

        def gen_time(ctime):
            return strftime('%H:%M', localtime(ctime))

        if len(log) <= LOG_LIMIT:
            ctime_list = [gen_time(x['ctime']) for x in log]
            count_list = [x['count'] for x in log]
        else:
            log = log[len(log) - LOG_LIMIT:]
            ctime_list = [gen_time(x['ctime']) for x in log]
            count_list = [x['count'] for x in log]

        data = dict(
            ctime_list=ctime_list,
            count_list=count_list,
        )

        self.write(json.dumps(data))

class OverlookCSVHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        csvname = self.get_argument('csvname', None)
        csvinfo = {
            'user':[db.user_log, 'user.log.{}.csv'],
            'source':[db.source_log, 'source.log.{}.csv'],
            'traffic':[db.traffic_log, 'traffic.log.{}.csv'],
        }.get(csvname, None)
        if csvinfo is None: self.send_error(400)

        self.set_header('Content-Type', 'application/octet-stream')
        self.set_header('Content-Disposition', 'attachment;filename=' + csvinfo[1].format(int(time())))

        def gen_line(item):
            _ctime = strftime('%Y-%m-%d %H:%M', localtime(item['ctime']))
            count = item['count']
            return '{ctime},{count}\n'.format(ctime=_ctime, count=count)

        def yield_line(collection):
            for item in collection.find():
                yield gen_line(item)

        self.write('ctime,count\n')
        for line in yield_line(csvinfo[0]):
            self.write(line)

        self.finish()

class IndexHandler(tornado.web.RequestHandler):
    def get(self):
        return self.render('index.html')

class IndexInfoHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        source_count = db.source.count()
        data = dict(
            source_count=source_count,
        )
        self.write(json.dumps(data))
        self.finish()

class DonateHandler(tornado.web.RequestHandler):
    def get(self):
        self.render('donate.html')

class TestChineseHandler(tornado.web.RequestHandler):
    def post(self):
        query_string = self.get_argument('query_string')
        query = QueryParser(Version.LUCENE_30, 'content', ANALYZER).parse(query_string)
        scorer = QueryScorer(query, 'content')
        highlighter = Highlighter(FORMATTER, scorer)
        highlighter.setTextFragmenter(SimpleSpanFragmenter(scorer))
        start_time = time()
        total_hits = testsearcher.search(query, RESULT_MAX_NUM)
        items = []

        for hit in total_hits.scoreDocs:
            doc = testsearcher.doc(hit.doc)
            content = doc.get('content')
            stream = TokenSources.getAnyTokenStream(testsearcher.getIndexReader(), hit.doc, 'content', doc, ANALYZER)
            content = highlighter.getBestFragment(stream, content)
            items.append(content)

        self.render('testchinese.html', content=TEST_CHINESE_CONTENT, items=items)

    def get(self):
        self.render('testchinese.html', content=TEST_CHINESE_CONTENT, items=None)

settings = dict(
    debug=True,
    template_path='template',
    static_path='static',
)

application = tornado.web.Application([
    (r'/', IndexHandler),
    (r'/testchinese', TestChineseHandler),
    (r'/info', IndexInfoHandler),
    (r'/donate', DonateHandler),
    (r'/search', SearchHandler),
    (r'/admin', OverlookHandler),
    (r'/admin/overlook', OverlookHandler),
    (r'/admin/overlook/chart', OverlookChartHandler),
    (r'/admin/overlook/csv', OverlookCSVHandler),
], **settings)

if __name__ == '__main__':
    # rebuild_indexing()
    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()
