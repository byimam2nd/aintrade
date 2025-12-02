
from sqlalchemy import create_engine, Column, String, BigInteger, DateTime
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func
import datetime

Base = declarative_base()

class Signal(Base):
    __tablename__ = 'signals'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    symbol = Column(String, nullable=False, index=True)
    signal = Column(String, nullable=False) # e.g., 'BUY', 'SELL', 'HOLD'
    strategy = Column(String, nullable=False, index=True) # e.g., 'enhanced_trend_master'
    timeframe = Column(String, nullable=False)
    # The timestamp when the signal was generated
    timestamp = Column(DateTime(timezone=True), server_default=func.now())
    # The open_time of the kline that triggered the signal
    kline_open_time = Column(BigInteger, nullable=False, index=True)

    def __repr__(self):
        return (
            f"<Signal(id={self.id}, symbol='{self.symbol}', signal='{self.signal}', "
            f"strategy='{self.strategy}', timeframe='{self.timeframe}', timestamp='{self.timestamp}')>"
        )

if __name__ == '__main__':
    engine = create_engine('sqlite:///database/signals.db')
    print("Creating database and table 'signals'...")
    Base.metadata.create_all(engine)
    print("Done.")
