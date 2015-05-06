from datetime import datetime
import json
import urllib
from pybloom import ScalableBloomFilter
import sqlite3
import re
from crawler import Command

__author__ = 'chen_xi'

# a = 'http://bbs.hupu.com/bxj-2'
# b = '/bxj-3'
# c = 'http://bbs.hupu.com/12622368.html'
#
# d = 'http://bbs.hupu.com/ent'
#
# t = 'http://bbs.hupu.com/b'
#
# # p = re.compile(r'http://bbs.hup.com/[bxj|bxj\-\d+|\d+\.html]')
# p = re.compile(r'http://bbs.hupu.com/(bxj(\-\d+)?|\d+\.html)$')
# # p = re.compile(r'/(bxj\-\d+)|(\d+\.html)$')
# # p = re.compile(r'/bxj\-\d+$')
# print(p.match(c))

a = 'http://www.waorksap.com'

p = re.compile(r'worksap')
print(p.search(a))

url = '/abc'
print(url.startswith('/'))

