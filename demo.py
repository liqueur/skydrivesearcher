#coding:utf-8

from sched import scheduler
from time import time, sleep
from twisted.internet import defer, reactor

s = scheduler(time, sleep)

def finish():
    sleep(1)
    run()

def callback(resp):
    print resp
    finish()

def run():
    d = defer.Deferred()
    d.addCallback(callback)
    d.callback('test')

if __name__ == '__main__':
    run()
    reactor.run()
