
from sqlalchemy import Column, BigInteger, Float

def create_kline_model(DynamicBase, table_name: str):
    """Dynamically creates a unique Kline model class for a given table name."""
    safe_table_name = "".join(c for c in table_name if c.isalnum())
    class_name = f"Kline_{safe_table_name}"

    # Use type() to create a new class with a unique name, avoiding registry conflicts
    kline_class = type(
        class_name,
        (DynamicBase,),
        {
            "__tablename__": safe_table_name,
            "open_time": Column(BigInteger, primary_key=True),
            "open": Column(Float),
            "high": Column(Float),
            "low": Column(Float),
            "close": Column(Float),
            "volume": Column(Float),
            "close_time": Column(BigInteger),
            "quote_asset_volume": Column(Float),
            "number_of_trades": Column(BigInteger),
            "taker_buy_base_asset_volume": Column(Float),
            "taker_buy_quote_asset_volume": Column(Float),
            "__repr__": lambda self: f"<{class_name}(T={self.open_time}, O={self.open})>"
        }
    )
    return kline_class
