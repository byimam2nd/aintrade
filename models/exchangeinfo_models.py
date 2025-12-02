
from sqlalchemy import create_engine, Column, String, Integer, Boolean, JSON
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class ExchangeInfo(Base):
    __tablename__ = 'exchange_info'

    symbol = Column(String, primary_key=True)
    status = Column(String)
    base_asset = Column(String)
    base_asset_precision = Column(Integer)
    quote_asset = Column(String)
    quote_precision = Column(Integer)
    quote_asset_precision = Column(Integer)
    base_commission_precision = Column(Integer)
    quote_commission_precision = Column(Integer)
    order_types = Column(JSON)
    iceberg_allowed = Column(Boolean)
    oco_allowed = Column(Boolean)
    oto_allowed = Column(Boolean)
    quote_order_qty_market_allowed = Column(Boolean)
    allow_trailing_stop = Column(Boolean)
    cancel_replace_allowed = Column(Boolean)
    is_spot_trading_allowed = Column(Boolean)
    is_margin_trading_allowed = Column(Boolean)
    filters = Column(JSON)
    permissions = Column(JSON)
    permission_sets = Column(JSON)
    default_self_trade_prevention_mode = Column(String)
    allowed_self_trade_prevention_modes = Column(JSON)


    def __repr__(self):
        return f"<ExchangeInfo(symbol='{self.symbol}', status='{self.status}')>"

if __name__ == '__main__':
    # This script can be run to create the database and table.
    engine = create_engine('sqlite:///database/exchangeinfo.db')
    print("Creating database and table 'exchange_info'...")
    Base.metadata.create_all(engine)
    print("Done.")
