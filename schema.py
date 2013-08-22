# coding:utf-8
'''
Table definitions
'''
from sqlalchemy import create_engine
from sqlalchemy import Column, DateTime, Integer, String, Numeric
from sqlalchemy.ext.declarative import declarative_base


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

    def __init__(self, datetime, symbol, open, high, low, close, volume):
        self.datetime = datetime
        self.symbol = symbol
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume

    def __repr__(self):
        return '<Quote (%s, %s, %f, %f, %f, %f, %d)>' % (self.symbol, self.datetime.strftime('%Y-%m-%d %H:%M:%S'), self.open, self.high, self.low, self.close, self.volume)


def create():
    Base.metadata.create_all(engine)
