#coding:utf-8

import lucene
import tornado.web
import tornado.ioloop
from pymongo import MongoClient
from time import localtime, strftime, time
from lucene import *

lucene.initVM()
db = MongoClient().sds
indexdir = SimpleFSDirectory(File('index'))
searcher = IndexSearcher(indexdir)
# analyzer = StandardAnalyzer(Version.LUCENE_30)
# analyzer = ChineseAnalyzer(Version.LUCENE_30)
analyzer = CJKAnalyzer(Version.LUCENE_30)
formatter = SimpleHTMLFormatter("<span class=\'highlight\'>", "</span>")

class IndexHandler(tornado.web.RequestHandler):
    def get(self):
        self.render('index.html')

class ResultHandler(tornado.web.RequestHandler):
    def post(self):
        query_string = self.get_argument('query')
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

settings = dict(
    debug=True,
    template_path='template',
    static_path='static',
)

application = tornado.web.Application([
    (r'/', IndexHandler),
    (r'/result', ResultHandler),
], **settings)

if __name__ == '__main__':
    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()
