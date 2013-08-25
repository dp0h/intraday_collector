#!/usr/bin/env python
# coding:utf-8
'''
Utility for fetching Google intraday market data.
'''
from __future__ import print_function
import os
import sys
import argparse
import logging
from datetime import datetime
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker
import csv
import schema
from schema import Quote
from google_finace import fetch_intraday_quotes


def load_symbols(fname):
    res = []
    with open(fname, 'rb') as f:
        reader = csv.reader(f)
        for x in reader:
            res.append(x[0])
    return res


def to_csv(symbol, values):
    ''' Export marketdata for specified symbol to csv file '''
    pass

LOG_DIR = './logs'


def fetch(file):
    if not os.path.isdir(LOG_DIR):
        os.mkdir(LOG_DIR)
    now = datetime.now()
    fname = '%d-%02d-%02d_%02d-%02d-%02d.log' % (now.year, now.month, now.day, now.hour, now.minute, now.second)
    logging.basicConfig(filename=os.path.join(LOG_DIR, fname), level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    schema.init()
    session = sessionmaker(bind=schema.engine)()

    sybols = load_symbols(file)
    logging.info('Processing file: %s, number of symbols: %d' % (file, len(sybols)))

    for s in sybols:
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


def output2csv(file):
    now = datetime.now()
    outpath = "./out-%d-%02d-%02d_%02d-%02d-%02d" % (now.year, now.month, now.day, now.hour, now.minute, now.second)
    os.makedirs(outpath)

    schema.init()
    session = sessionmaker(bind=schema.engine)()

    sybols = load_symbols(file)
    for s in sybols:
        res = session.query(Quote).filter(Quote.symbol == s).order_by(Quote.datetime)
        to_csv(s, res)
        break  # TODO


def main(symbols_file, output):
    if not output:
        fetch(symbols_file)
    else:
        output2csv(symbols_file)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Fetches Google intraday market data.')
    parser.add_argument('-o', '--output', action='store_true', help='flag which indicates if output csv should be generated')
    parser.add_argument('symbols', metavar='symbols', type=str, help='a file with symbols')

    args = parser.parse_args()
    main(args.symbols, args.output)
