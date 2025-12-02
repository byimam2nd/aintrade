
import asyncio
import json
import logging
import configparser
import os
import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import OperationalError
import websockets

# Add the root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.base_symbols_models import Symbol as FilteredSymbol
from models.dynamic_models import create_kline_model

# --- Configuration ---
SOURCE_DB_URL = 'sqlite:///database/filtered_tradable_symbols.db'
HISTORICAL_DB_DIR = 'database/historical_filtered_symbols'
TIME_FRAMES = ["15m", "1h", "4h"]
BASE_STREAM_URL = "wss://stream.binance.com:9443/stream?streams="

# --- Logging Setup ---
config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.ini')
config.read(config_path)

logging_level_str = config.get('logging', 'level', fallback='INFO').upper()
logging_level = getattr(logging, logging_level_str, logging.INFO)
logging.basicConfig(level=logging_level, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Global cache for database engines and models ---
# This avoids recreating them for every single message
db_engines = {}
kline_models = {}

def get_db_engine(symbol):
    if symbol not in db_engines:
        db_path = os.path.join(HISTORICAL_DB_DIR, f'{symbol}.db')
        db_engines[symbol] = create_engine(f'sqlite:///{db_path}')
    return db_engines[symbol]

def get_kline_model(symbol, timeframe):
    key = f"{symbol}_{timeframe}"
    if key not in kline_models:
        # Each model needs a unique base to avoid metadata conflicts
        Base = declarative_base()
        kline_models[key] = create_kline_model(Base, timeframe)
    return kline_models[key]

def handle_kline_message(msg):
    """Processes a single k-line message from the WebSocket."""
    try:
        data = json.loads(msg)
        stream_name = data.get('stream')
        kline_data = data.get('data')

        if not stream_name or not kline_data:
            return

        kline = kline_data.get('k')
        # We only care about closed candles
        if not kline or not kline.get('x'):
            return

        symbol = kline['s']
        interval = kline['i']
        
        logger.debug(f"Received closed kline for {symbol} [{interval}]")

        engine = get_db_engine(symbol)
        KlineModel = get_kline_model(symbol, interval)
        
        Session = sessionmaker(bind=engine)
        session = Session()
        try:
            kline_obj = KlineModel(
                open_time=kline['t'],
                open=float(kline['o']),
                high=float(kline['h']),
                low=float(kline['l']),
                close=float(kline['c']),
                volume=float(kline['v']),
                close_time=kline['T'],
                quote_asset_volume=float(kline['q']),
                number_of_trades=kline['n'],
                taker_buy_base_asset_volume=float(kline['V']),
                taker_buy_quote_asset_volume=float(kline['Q'])
            )
            session.merge(kline_obj)
            session.commit()
            logger.info(f"Updated kline for {symbol} [{interval}] at {datetime.fromtimestamp(kline['t']/1000)}")
        except Exception as e:
            logger.error(f"DB Error processing {symbol} [{interval}]: {e}")
            session.rollback()
        finally:
            session.close()

    except json.JSONDecodeError:
        logger.warning(f"Could not decode JSON from message: {msg}")
    except Exception as e:
        logger.error(f"Error in handle_kline_message: {e}", exc_info=True)


async def connect_and_stream(stream_url):
    """Connects to the WebSocket and processes messages."""
    logger.info(f"Connecting to {len(stream_url.split('/')) -1} kline streams...")
    while True:
        try:
            async with websockets.connect(stream_url, ping_interval=60, ping_timeout=20) as ws:
                logger.info("WebSocket connected successfully.")
                while True:
                    message = await ws.recv()
                    handle_kline_message(message)
        except websockets.ConnectionClosed:
            logger.warning("WebSocket disconnected, reconnecting...")
        except Exception as e:
            logger.error(f"WebSocket connection error: {e}")
        
        logger.info("Retrying in 10 seconds...")
        await asyncio.sleep(10)

def main():
    """Main function to set up and run the streaming agent."""
    logger.info("====== Starting Real-time K-line Streaming Agent ======")
    
    # 1. Get the list of symbols to process
    source_engine = create_engine(SOURCE_DB_URL)
    SourceSession = sessionmaker(bind=source_engine)
    session = SourceSession()
    try:
        symbols = [s.symbol.lower() for s in session.query(FilteredSymbol).all()]
        if not symbols:
            logger.warning(f"No symbols found in '{SOURCE_DB_URL}'. Exiting.")
            return
        logger.info(f"Found {len(symbols)} symbols to stream.")
    except OperationalError:
        logger.error(f"Could not read from '{SOURCE_DB_URL}'. Run filtering_agent.py first.")
        return
    finally:
        session.close()

    # 2. Construct the stream URL
    streams = [f"{symbol}@kline_{tf}" for symbol in symbols for tf in TIME_FRAMES]
    stream_url = BASE_STREAM_URL + "/".join(streams)
    
    # Run the streaming client
    try:
        asyncio.run(connect_and_stream(stream_url))
    except KeyboardInterrupt:
        logger.info("Streaming stopped by user.")

if __name__ == "__main__":
    main()