
import asyncio
import json
import logging
import datetime
import websockets
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.base_symbols_models import Base, Symbol
import configparser
import os

# Load config for logging setup
config_logging = configparser.ConfigParser()
config_logging_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.ini')
config_logging.read(config_logging_path)
logging_level_str = config_logging.get('logging', 'level', fallback='INFO').upper()
logging_level = getattr(logging, logging_level_str, logging.INFO)

# Logging setup
logging.basicConfig(
    level=logging_level,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load config (main config for agent operations)
config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.ini')
config.read(config_path)
quote_asset = config.get("binance", "quote_asset", fallback="USDT")
db_url = config.get("database", "url", fallback="sqlite:///database/base_symbols.db")
streaming_url = config.get("binance", "streaming_url", fallback="wss://stream.binance.com:9443/ws/!ticker@arr")

# Database setup
engine = create_engine(db_url)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)


def handle_message(msg_list):
    """Process list of tickers"""
    session = Session()
    updated_symbols_count = 0
    try:
        symbols_to_update = []
        for ticker in msg_list:
            # Apply filter unless quote_asset is 'ALL'
            if quote_asset != "ALL" and not ticker["s"].endswith(quote_asset):
                continue

            # Append to list for processing
            symbols_to_update.append(ticker)
        
        if not symbols_to_update:
            return

        for ticker in symbols_to_update:
            symbol = session.query(Symbol).filter_by(symbol=ticker["s"]).first()
            if not symbol:
                symbol = Symbol(symbol=ticker["s"])

            symbol.price_change = ticker["p"]
            symbol.price_change_percent = ticker["P"]
            symbol.weighted_avg_price = ticker["w"]
            symbol.prev_close_price = ticker["x"]
            symbol.last_price = ticker["c"]
            symbol.last_qty = ticker["Q"]
            symbol.bid_price = ticker["b"]
            symbol.ask_price = ticker["a"]
            symbol.open_price = ticker["o"]
            symbol.high_price = ticker["h"]
            symbol.low_price = ticker["l"]
            symbol.volume = ticker["v"]
            symbol.quote_volume = ticker["q"]
            symbol.open_time = ticker["O"]
            symbol.close_time = ticker["C"]
            symbol.first_id = ticker["F"]
            symbol.last_id = ticker["L"]
            symbol.count = ticker["n"]
            symbol.last_updated = datetime.datetime.now(datetime.timezone.utc)

            session.merge(symbol)
            updated_symbols_count += 1

        session.commit()
        logging.info(f"Received {len(msg_list)} tickers, processed and updated {updated_symbols_count} symbols.")

    except Exception as e:
        logging.error(f"DB Handler Error: {e}")
        session.rollback()
    finally:
        session.close()


async def connect_and_stream(run_for_seconds=None):
    """Stable WS connection with infinite retry, with an optional exit timer."""
    retry_delay = 5
    start_time = asyncio.get_event_loop().time()

    while True:
        # Check for exit condition
        if run_for_seconds and (asyncio.get_event_loop().time() - start_time) > run_for_seconds:
            logging.info(f"Run timer of {run_for_seconds} seconds expired. Exiting.")
            break

        try:
            logging.info(f"Connecting to {streaming_url} ...")

            async with websockets.connect(streaming_url, ping_interval=20, ping_timeout=20) as ws:
                logging.info("WebSocket connected successfully.")

                while True:
                    # Check for exit condition inside the loop as well
                    if run_for_seconds and (asyncio.get_event_loop().time() - start_time) > run_for_seconds:
                        break
                    
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        data = json.loads(message)

                        if isinstance(data, list):
                            handle_message(data)
                        else:
                            logging.warning(f"Unknown message format: {data}")

                    except asyncio.TimeoutError:
                        continue # No message received, just loop again
                    except websockets.ConnectionClosed:
                        logging.warning("WebSocket disconnected, reconnecting...")
                        break
                    except Exception as e:
                        logging.error(f"Error in message loop: {e}")

        except Exception as e:
            logging.error(f"WebSocket connection error: {e}")

        if run_for_seconds and (asyncio.get_event_loop().time() - start_time) > run_for_seconds:
            break
            
        logging.info(f"Retrying in {retry_delay} seconds...")
        await asyncio.sleep(retry_delay)


def main(run_for_seconds=None):
    """
    Main function to run the streaming agent.
    
    Args:
        run_for_seconds (int, optional): If provided, the agent will run for this
                                         many seconds and then exit. Useful for setup phase.
    """
    try:
        asyncio.run(connect_and_stream(run_for_seconds))
    except KeyboardInterrupt:
        logging.info("Stopped by user.")


if __name__ == "__main__":
    # To run this for a limited time (e.g., 60 seconds for setup), you could
    # call main(60) from another script. By default, it runs forever.
    main()
