#!/usr/bin/env python
# coding:utf-8
'''
Utility for fetching Google intraday market data.
'''
from __future__ import print_function
import os
import argparse
import logging
from datetime import datetime
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker
import csv
import schema
from schema import Quote
from google_finace import fetch_intraday_quotes


def _load_symbols(fname):
    with open(fname, 'rb') as f:
        return [x.rstrip() for x in f.readlines()]


def _to_csv(fname, values):
    ''' Export marketdata for specified symbol to csv file '''
    with open(fname, 'wb') as f:
        writer = csv.writer(f)
        writer.writerow(['Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume'])
        for x in values:
            writer.writerow([x.datetime.date(), x.datetime.time(), x.open, x.high, x.low, x.close, x.volume])


def _now():
    now = datetime.now()
    return '%d-%02d-%02d_%02d-%02d-%02d' % (now.year, now.month, now.day, now.hour, now.minute, now.second)

_LOG_DIR = './logs'


def fetch(symbols, session):
    if not os.path.isdir(_LOG_DIR):
        os.mkdir(_LOG_DIR)
    fname = '%s.log' % _now()
    logging.basicConfig(filename=os.path.join(_LOG_DIR, fname), level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    logging.info('Processing file: %s, number of symbols: %d' % (file, len(symbols)))
    for s in symbols:
        try:
            quotes = fetch_intraday_quotes(s)
            if quotes:
                last_date = session.query(func.max(Quote.datetime)).filter_by(symbol=s).all()[0][0]
                add = [Quote(*q) for q in quotes if q[1] > last_date] if last_date else [Quote(*q) for q in quotes]
                session.add_all(add)
                session.commit()
                logging.info('%s: updated %d from %d' % (s, len(add), len(quotes)))
            else:
                logging.info('No marketdata retrieved for symbol %s' % s)
        except Exception as e:
            logging.error('Failed to update symbol %s, error: %s' % (s, str(e)))
            session.rollback()


def output2csv(symbols, session):
    outpath = "./out-%s" % _now()
    os.makedirs(outpath)

    for s in symbols:
        qres = session.query(Quote).filter(Quote.symbol == s).order_by(Quote.datetime)
        _to_csv(os.path.join(outpath, '%s.csv' % s), qres)


def main(symbols_file, output):
    symbols = _load_symbols(symbols_file)

    schema.init()
    dbsession = sessionmaker(bind=schema.engine)()

    if not output:
        fetch(symbols, dbsession)
    else:
        output2csv(symbols, dbsession)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Fetches Google intraday market data.')
    parser.add_argument('-o', '--output', action='store_true', help='flag which indicates if output csv should be generated')
    parser.add_argument('symbols', metavar='symbols', type=str, help='a file with symbols')

    args = parser.parse_args()
    main(args.symbols, args.output)
