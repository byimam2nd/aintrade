
import requests
import logging
import configparser
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sys
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

# Add the root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.exchangeinfo_models import Base, ExchangeInfo

def fetch_exchange_info(config):
    """Fetches exchange information from the Binance API."""
    api_url = config.get("binance", "exchange_info_url")
    try:
        logging.info(f"Fetching data from {api_url}")
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        logging.info("Data fetched successfully.")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        return None

def store_exchange_info(data, config):
    """Stores the fetched exchange information into the database."""
    if not data or 'symbols' not in data:
        logging.warning("No symbol data to store.")
        return

    db_url = "sqlite:///database/exchangeinfo.db" # As per original request, this agent has its own DB
    quote_asset = config.get("binance", "quote_asset")
    
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        symbols_data = data['symbols']
        logging.info(f"Received {len(symbols_data)} symbols from API.")

        # Filter symbols based on quote_asset from config
        if quote_asset != "ALL":
            symbols_data = [s for s in symbols_data if s.get('quoteAsset') == quote_asset]
            logging.info(f"Filtered down to {len(symbols_data)} symbols for quote asset '{quote_asset}'.")

        if not symbols_data:
            logging.warning("No symbols to process after filtering.")
            return

        logging.info(f"Processing and storing {len(symbols_data)} symbols...")
        for symbol_data in symbols_data:
            exchange_info_entry = ExchangeInfo(
                symbol=symbol_data.get('symbol'),
                status=symbol_data.get('status'),
                base_asset=symbol_data.get('baseAsset'),
                base_asset_precision=symbol_data.get('baseAssetPrecision'),
                quote_asset=symbol_data.get('quoteAsset'),
                quote_precision=symbol_data.get('quotePrecision'),
                quote_asset_precision=symbol_data.get('quoteAssetPrecision'),
                base_commission_precision=symbol_data.get('baseCommissionPrecision'),
                quote_commission_precision=symbol_data.get('quoteCommissionPrecision'),
                order_types=symbol_data.get('orderTypes'),
                iceberg_allowed=symbol_data.get('icebergAllowed'),
                oco_allowed=symbol_data.get('ocoAllowed'),
                oto_allowed=symbol_data.get('otoAllowed'),
                quote_order_qty_market_allowed=symbol_data.get('quoteOrderQtyMarketAllowed'),
                allow_trailing_stop=symbol_data.get('allowTrailingStop'),
                cancel_replace_allowed=symbol_data.get('cancelReplaceAllowed'),
                is_spot_trading_allowed=symbol_data.get('isSpotTradingAllowed'),
                is_margin_trading_allowed=symbol_data.get('isMarginTradingAllowed'),
                filters=symbol_data.get('filters'),
                permissions=symbol_data.get('permissions'),
                permission_sets=symbol_data.get('permissionSets'),
                default_self_trade_prevention_mode=symbol_data.get('defaultSelfTradePreventionMode'),
                allowed_self_trade_prevention_modes=symbol_data.get('allowed_self_trade_prevention_modes')
            )
            session.merge(exchange_info_entry)

        session.commit()
        logging.info(f"Successfully stored/updated data for {len(symbols_data)} symbols.")

    except Exception as e:
        logging.error(f"Database error: {e}")
        session.rollback()
    finally:
        session.close()

def run_agent():
    """Main function to run the agent."""
    logging.info("--- Starting ExchangeInfo Agent ---")
    
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.ini')
    config.read(config_path)

    exchange_data = fetch_exchange_info(config)
    if exchange_data:
        store_exchange_info(exchange_data, config)
    logging.info("--- ExchangeInfo Agent Finished ---")

if __name__ == '__main__':
    run_agent()
