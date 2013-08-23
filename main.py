#!/usr/bin/env python
# coding:utf-8
'''
'''
import os
import logging
from datetime import datetime
import numpy as np
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
import schema
from google_finace import fetch_intraday_quotes


def load_symbols(fname):
    return np.loadtxt(fname, dtype='S10', comments='#', skiprows=0)


def to_csv(symbol):
    ''' Export marketdata for specified symbol to csv file '''
    pass

LOG_DIR = './logs'


def main(files):
    if not os.path.isdir(LOG_DIR):
        os.mkdir(LOG_DIR)
    now = datetime.now()
    fname = '%d-%02d-%02d_%02d-%02d-%02d.log' % (now.year, now.month, now.day, now.hour, now.minute, now.second)
    logging.basicConfig(filename=os.path.join(LOG_DIR, fname), level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    schema.init()
    session = sessionmaker(bind=schema.engine)()

    for file in files:
        sybols = load_symbols(file)

        for s in sybols:
            updated = 0
            quotes = fetch_intraday_quotes(s)
            for q in quotes:
                try:
                    #TODO: fetch latest datatime from DB for symbol
                    session.add_all([schema.Quote(*q)])
                    session.commit()
                    updated += 1
                except IntegrityError:
                    session.rollback()
            logging.info('%s: updated %d from %d' % (s, updated, len(quotes)))
            break
        break


if __name__ == '__main__':
    main(['idx/sp500.dat', 'idx/ftse100.dat'])
