# coding:utf-8
'''
Table definitions
'''
import logging
from sqlalchemy import create_engine
from sqlalchemy import Column, DateTime, Integer, String, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.reflection import Inspector


engine = create_engine('sqlite:///marketdata.db', echo=False)
Base = declarative_base()


class Quote(Base):
    __tablename__ = 'quotes'

    symbol = Column(String, primary_key=True)
    datetime = Column(DateTime, primary_key=True)
    open = Column(Numeric(12, 2))
    high = Column(Numeric(12, 2))
    low = Column(Numeric(12, 2))
    close = Column(Numeric(12, 2))
    volume = Column(Integer)

    def __init__(self, symbol, datetime, open, high, low, close, volume):
        self.symbol = symbol
        self.datetime = datetime
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume

    def __repr__(self):
        return '<Quote (%s, %s, %f, %f, %f, %f, %d)>' % (self.symbol, self.datetime.strftime('%Y-%m-%d %H:%M:%S'), self.open, self.high, self.low, self.close, self.volume)


def create():
    Base.metadata.create_all(engine)
    logging.info('Database created')


def init():
    inspector = Inspector.from_engine(engine)
    if not Quote.__tablename__ in inspector.get_table_names():
        create()
