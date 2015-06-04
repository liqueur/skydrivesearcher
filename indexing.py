#coding:utf-8

import lucene
from lucene import *
from settings import TEST_CHINESE_CONTENT, ANALYZER

initVM()
indexdir = SimpleFSDirectory(File('testindex'))
writer = IndexWriter(indexdir, ANALYZER, True, IndexWriter.MaxFieldLength.UNLIMITED)
doc = Document()
doc.add(Field('content', TEST_CHINESE_CONTENT, Field.Store.YES, Field.Index.ANALYZED))
writer.addDocument(doc)
writer.close()
