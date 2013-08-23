#!/usr/bin/env python
# coding:utf-8
'''
'''
import os
import logging
from datetime import datetime
import numpy as np
from sqlalchemy.orm import sessionmaker
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
        logging.info('Processing file: %s, number of symbols: %d' % (file, len(sybols)))

        for s in sybols:
            try:
                quotes = fetch_intraday_quotes(s)
                if quotes:
                    #TODO: filter already existed quotes
                    add = [schema.Quote(*q) for q in quotes]
                    session.add_all(add)
                    session.commit()
                else:
                    logging.infon('No marketdata retrieved for symbol %s' % s)
                logging.info('%s: updated %d from %d' % (s, len(add), len(quotes)))
            except Exception as e:
                logging.error('Failed to update symbol %s, error: %s' % (s, str(e)))
                session.rollback()
            break


if __name__ == '__main__':
    main(['idx/sp500.dat', 'idx/ftse100.dat'])
