
import configparser
import logging
import requests
from sqlalchemy import create_engine, Column, BigInteger, Float
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import OperationalError
import sys
import os
import time
from datetime import datetime, timedelta

# Add the root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.base_symbols_models import Symbol as FilteredSymbol

# --- Configuration ---
SOURCE_DB_URL = 'sqlite:///database/filtered_tradable_symbols.db'
HISTORICAL_DB_DIR = 'database/historical_filtered_symbols'
TIME_FRAMES = ["15m", "1h", "4h"]
BINANCE_API_URL = "https://api.binance.com/api/v3/klines"
KLINE_LIMIT = 1000

# --- Logging Setup ---
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.propagate = False
if not logger.handlers:
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

# --- Dynamic Model Creation ---
def create_kline_model(DynamicBase, table_name: str):
    """Dynamically creates a unique Kline model class for a given table name."""
    safe_table_name = "".join(c for c in table_name if c.isalnum())
    class_name = f"Kline_{safe_table_name}"

    # Use type() to create a new class with a unique name, avoiding registry conflicts
    return type(
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

def fetch_klines(symbol, interval, start_time=None):
    """Fetches k-lines from Binance API."""
    params = {'symbol': symbol, 'interval': interval, 'limit': KLINE_LIMIT}
    if start_time:
        params['startTime'] = start_time
    logger.debug(f"Fetching {symbol} {interval} klines with params: {params}")
    try:
        response = requests.get(BINANCE_API_URL, params=params, timeout=15)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"API Error fetching {symbol} {interval}: {e}")
        return []

def run_historical_klines_agent():
    """Main agent to download and store historical k-line data."""
    logger.info("====== Starting Historical K-lines Agent ======")
    
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.ini')
    config.read(config_path)

    backfill_days = config.getint('historical_data', 'backfill_days', fallback=60)
    
    source_engine = create_engine(SOURCE_DB_URL)
    SourceSession = sessionmaker(bind=source_engine)
    source_session = SourceSession()
    try:
        symbols = [s.symbol for s in source_session.query(FilteredSymbol).all()]
        if not symbols:
            logger.warning(f"No symbols found in '{SOURCE_DB_URL}'. Exiting.")
            return
        logger.info(f"Found {len(symbols)} symbols to process: {symbols[:5]}...")
    except OperationalError:
        logger.error(f"Could not read from '{SOURCE_DB_URL}'. Please run the filtering_agent.py first.")
        return
    finally:
        source_session.close()

    for symbol in symbols:
        db_path = os.path.join(HISTORICAL_DB_DIR, f'{symbol}.db')
        logger.info(f"--- Processing symbol: {symbol} | Database: {db_path} ---")
        
        SymbolBase = declarative_base()
        symbol_engine = create_engine(f'sqlite:///{db_path}')
        SymbolSession = sessionmaker(bind=symbol_engine)
        
        for tf in TIME_FRAMES:
            session = SymbolSession()
            try:
                KlineModel = create_kline_model(SymbolBase, tf)
                SymbolBase.metadata.create_all(symbol_engine)

                last_kline = session.query(KlineModel).order_by(KlineModel.open_time.desc()).first()
                start_time = None
                
                if last_kline:
                    start_time = last_kline.open_time + 1
                    logger.info(f"[{tf}] Last entry found. Fetching new data since {datetime.fromtimestamp(start_time/1000)}.")
                else:
                    start_dt = datetime.now() - timedelta(days=backfill_days)
                    start_time = int(start_dt.timestamp() * 1000)
                    logger.info(f"[{tf}] No existing data. Backfilling data for last {backfill_days} days.")

                while True:
                    klines_data = fetch_klines(symbol, tf, start_time)
                    if not klines_data:
                        break

                    for kline in klines_data:
                        kline_obj = KlineModel(
                            open_time=kline[0], open=float(kline[1]), high=float(kline[2]),
                            low=float(kline[3]), close=float(kline[4]), volume=float(kline[5]),
                            close_time=kline[6], quote_asset_volume=float(kline[7]),
                            number_of_trades=kline[8], taker_buy_base_asset_volume=float(kline[9]),
                            taker_buy_quote_asset_volume=float(kline[10])
                        )
                        session.merge(kline_obj)
                    
                    session.commit()
                    logger.info(f"[{tf}] Stored {len(klines_data)} klines.")
                    start_time = klines_data[-1][0] + 1
                    
                    if len(klines_data) < KLINE_LIMIT:
                        break
                    time.sleep(0.5)

            except Exception as e:
                logger.error(f"Failed to process {symbol} {tf}: {e}", exc_info=True)
                session.rollback()
            finally:
                session.close()
    
    logger.info("====== Historical K-lines Agent Finished ======")

if __name__ == '__main__':
    run_historical_klines_agent()
