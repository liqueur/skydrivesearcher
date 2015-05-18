#coding:utf-8

import lucene
from lucene import *
from time import time
from pymongo import MongoClient

lucene.initVM()

db = MongoClient().sds
items = list(db.source.find({'name':'baidu'}, {'list':1}))[0]['list']
indexdir = SimpleFSDirectory(File('index'))
# analyzer = StandardAnalyzer(Version.LUCENE_30)
# analyzer = ChineseAnalyzer(Version.LUCENE_30)
analyzer = CJKAnalyzer(Version.LUCENE_30)
writer = IndexWriter(indexdir, analyzer, True, IndexWriter.MaxFieldLength.UNLIMITED)

for item in items:
    doc = Document()
    doc.add(Field('title', item['title'], Field.Store.YES, Field.Index.ANALYZED))
    doc.add(Field('url', item['url'], Field.Store.YES, Field.Index.NOT_ANALYZED))
    doc.add(Field('time', str(item['time']), Field.Store.YES, Field.Index.NOT_ANALYZED))
    writer.addDocument(doc)

writer.close()

searcher = IndexSearcher(indexdir)
kw = raw_input('keyword:')
while kw.strip() != '':
    # 设置查询语句解析
    query = QueryParser(Version.LUCENE_30, 'title', analyzer).parse(kw)
    # 设置高亮标签
    formatter = SimpleHTMLFormatter("<span class=\'highlight\'>", "</span>")
    scorer = QueryScorer(query, 'title')
    # 设置高亮器
    highlighter = Highlighter(formatter, scorer)
    highlighter.setTextFragmenter(SimpleSpanFragmenter(scorer))
    start = time()
    # 查询索引
    total_hits = searcher.search(query, 1000)
    print time() - start
    for hit in total_hits.scoreDocs:
        print "Hit Score: ",hit.score, "Hit Doc:",hit.doc, "HitString:",hit.toString()
        doc= searcher.doc(hit.doc)
        title = doc.get("title")
        stream = TokenSources.getAnyTokenStream(searcher.getIndexReader(), hit.doc, 'title', doc, analyzer)
        # 打印搜索结果段落
        print highlighter.getBestFragment(stream, title)

    kw = raw_input('keyword:')
