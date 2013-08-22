# coding:utf-8
'''
Functionality for fetching intraday quotes from Google finance
'''
from __future__ import print_function
import urllib
from datetime import datetime


def fetch_intraday_quotes(symbol, interval_seconds=60, num_days=10):
    ''' Fetches intraday quotes from Google finance '''
    url = "http://www.google.com/finance/getprices?q=%s&i=%d&p=%dd&f=d,o,h,l,c,v" % (symbol, interval_seconds, num_days)
    try:
        csv = urllib.urlopen(url).readlines()
    except IOError as e:
        print(e)
        return None
    res = []
    for i in xrange(7, len(csv)):
        if csv[i].count(',') != 5:
            continue
        off, c, h, l, o, vol = csv[i].split(',')
        if off[0] == 'a':
            day = float(off[1:])
            off = 0
        else:
            off = float(off)
        o, h, l, c = [float(x) for x in [o, h, l, c]]
        dt = datetime.fromtimestamp(day + (interval_seconds * off))
        res.append((symbol, dt, o, h, l, c, vol))
    return res
