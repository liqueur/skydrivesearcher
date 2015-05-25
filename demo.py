#coding:utf-8

from functools import partial

def foo(a, b, c=None):
    if c is None: c = 10
    return (a + b) * c

print foo.__name__
from IPython import embed
embed()
