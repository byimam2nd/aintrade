from sqlalchemy import create_engine, Column, String, BigInteger, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

class Symbol(Base):
    __tablename__ = 'symbols'

    symbol = Column(String, primary_key=True)
    price_change = Column(String)
    price_change_percent = Column(String)
    weighted_avg_price = Column(String)
    prev_close_price = Column(String)
    last_price = Column(String)
    last_qty = Column(String)
    bid_price = Column(String)
    ask_price = Column(String)
    open_price = Column(String)
    high_price = Column(String)
    low_price = Column(String)
    volume = Column(String)
    quote_volume = Column(String)
    open_time = Column(BigInteger)
    close_time = Column(BigInteger)
    first_id = Column(BigInteger)
    last_id = Column(BigInteger)
    count = Column(BigInteger)
    last_updated = Column(DateTime(timezone=True), onupdate=func.now())

    def __repr__(self):
        return f"<Symbol(symbol='{self.symbol}', last_price='{self.last_price}')>"

if __name__ == '__main__':
    engine = create_engine('sqlite:///database/base_symbols.db')
    Base.metadata.create_all(engine)
