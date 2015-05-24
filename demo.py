#coding:utf-8

from time import sleep

def gen(n):
    for x in range(0, n, 24):
        yield x

for x in gen(240):
    print x
