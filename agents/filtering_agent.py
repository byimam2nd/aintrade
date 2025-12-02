
import configparser
import logging
import json
from sqlalchemy import create_engine, cast, Float, func, BigInteger, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
import sys
import os
import configparser

# Add the root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.base_symbols_models import Base as BaseSymbolsBase, Symbol
from models.exchangeinfo_models import Base as ExchangeInfoBase, ExchangeInfo

# --- Configuration ---
config = configparser.ConfigParser()
DEST_DB_URL = 'sqlite:///database/filtered_tradable_symbols.db'
EXCHANGE_INFO_DB_URL = 'sqlite:///database/exchangeinfo.db'

# --- Logging Setup ---
# Use a logger instance
logger = logging.getLogger(__name__)
# Read logging level from config.ini
logging_level_str = config.get('logging', 'level', fallback='INFO').upper()
logging_level = getattr(logging, logging_level_str, logging.INFO)
logger.setLevel(logging_level)
# Prevent propagation to the root logger
logger.propagate = False
# Add handler if not already present
if not logger.handlers:
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)


def get_stage1_symbols(config, session):
    """
    Filters symbols from base_symbols.db based on '[filtering]' section.
    Returns a list of Symbol objects.
    """
    logger.info("--- Starting Stage 1: Filtering from base_symbols.db ---")
    query = session.query(Symbol)
    filters = config['filtering']
    active_filters = []

    # Symbol Name
    symbol_contains_val = filters.get('symbol_contains', '').strip().upper()
    if symbol_contains_val:
        query = query.filter(Symbol.symbol.contains(symbol_contains_val))
        active_filters.append(f"Symbol contains '{symbol_contains_val}'")
    
    min_pcp = float(filters.get('min_price_change_percent', '0'))
    max_pcp = float(filters.get('max_price_change_percent', '0'))
    if min_pcp > 0 or max_pcp > 0:
        price_change_abs = func.abs(cast(Symbol.price_change_percent, Float))
        if min_pcp > 0 and max_pcp > 0: query = query.filter(price_change_abs.between(min_pcp, max_pcp)); active_filters.append(f"Price Change % between {min_pcp}%-{max_pcp}%")
        elif min_pcp > 0: query = query.filter(price_change_abs >= min_pcp); active_filters.append(f"Price Change % >= {min_pcp}%")
        elif max_pcp > 0: query = query.filter(price_change_abs <= max_pcp); active_filters.append(f"Price Change % <= {max_pcp}%")
    
    max_spread = float(filters.get('max_spread_percent', '0'))
    if max_spread > 0:
        spread_expr = (cast(Symbol.ask_price, Float) - cast(Symbol.bid_price, Float)) / cast(Symbol.bid_price, Float) * 100
        query = query.filter(cast(Symbol.bid_price, Float) > 0, spread_expr <= max_spread)
        active_filters.append(f"Spread <= {max_spread}%")

    numeric_fields = ['price_change','weighted_avg_price','prev_close_price','last_price','last_qty','bid_price','ask_price','open_price','high_price','low_price','volume','quote_volume']
    bigint_fields = ['open_time','close_time','first_id','last_id','count']
    for field in numeric_fields:
        min_val, max_val = float(filters.get(f'min_{field}','0')), float(filters.get(f'max_{field}','0'))
        if min_val > 0: query = query.filter(cast(getattr(Symbol, field), Float) >= min_val); active_filters.append(f"min_{field} >= {min_val}")
        if max_val > 0: query = query.filter(cast(getattr(Symbol, field), Float) <= max_val); active_filters.append(f"max_{field} <= {max_val}")
    for field in bigint_fields:
        min_val, max_val = int(filters.get(f'min_{field}','0')), int(filters.get(f'max_{field}','0'))
        if min_val > 0: query = query.filter(cast(getattr(Symbol, field), BigInteger) >= min_val); active_filters.append(f"min_{field} >= {min_val}")
        if max_val > 0: query = query.filter(cast(getattr(Symbol, field), BigInteger) <= max_val); active_filters.append(f"max_{field} <= {max_val}")

    logger.info("Stage 1 Active Filters: " + (", ".join(active_filters) if active_filters else "None"))
    return query.all()

def _get_min_notional_from_filters(filters_json):
    """Safely parse filters JSON and extract minNotional value."""
    if filters_json is None: return None
    
    filters_list = filters_json
    if isinstance(filters_json, str):
        try:
            filters_list = json.loads(filters_json)
        except json.JSONDecodeError:
            return None # Malformed JSON
            
    for f in filters_list:
        if isinstance(f, dict) and f.get('filterType') == 'NOTIONAL':
            return float(f.get('minNotional', 0.0))
    return None

def get_stage2_symbols(config):
    """
    Filters symbols from exchangeinfo.db based on '[exchange_filtering]' with detailed logging.
    """
    logger.info("--- Starting Stage 2: Filtering from exchangeinfo.db ---")
    exchange_filters_config = config['exchange_filtering']
    engine = create_engine(EXCHANGE_INFO_DB_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    passed_symbols = set()
    active_filters_log = [f"{key}='{value}'" for key, value in exchange_filters_config.items() if value and value != '0']
    logger.info("Stage 2 Active Filters: " + (", ".join(active_filters_log) if active_filters_log else "None"))

    try:
        all_exchange_info = session.query(ExchangeInfo).all()
        if not all_exchange_info:
            logger.warning("The 'exchange_info' table is empty. Cannot perform Stage 2 filtering.")
            return set()

        for info in all_exchange_info:
            rejection_reasons = []

            # String equals filters
            for key, col_name in [('status_equals','status'), ('base_asset_equals','base_asset'), ('quote_asset_equals','quote_asset')]:
                val = exchange_filters_config.get(key, '').strip().upper()
                if val and getattr(info, col_name) != val:
                    rejection_reasons.append(f"{key} mismatch (is '{getattr(info, col_name)}', needs '{val}')")
            
            # Boolean filters
            for key in ['is_spot_trading_allowed', 'is_margin_trading_allowed']:
                val = exchange_filters_config.get(key, '').strip()
                current_val = getattr(info, key, None)
                if val and (current_val is None or bool(int(val)) != current_val):
                    rejection_reasons.append(f"{key} mismatch (is '{current_val}', needs '{bool(int(val))}')")
            
            # max_allowed_min_notional check
            max_allowed = exchange_filters_config.getfloat('max_allowed_min_notional', 0.0)
            if max_allowed > 0:
                actual_min_notional = _get_min_notional_from_filters(info.filters)
                if actual_min_notional is None:
                    rejection_reasons.append("minNotional filter not found in symbol data")
                elif actual_min_notional > max_allowed:
                    rejection_reasons.append(f"minNotional too high ({actual_min_notional} > {max_allowed})")

            # --- Final check ---
            if not rejection_reasons:
                passed_symbols.add(info.symbol)
            else:
                logger.debug(f"Rejected {info.symbol}: " + ", ".join(rejection_reasons))

        logger.info(f"Stage 2 completed: Found {len(passed_symbols)} symbols passing exchange info filters.")
        return passed_symbols
    finally:
        session.close()


def run_filtering_agent():
    """ Orchestrates the two-stage filtering process with improved error handling. """
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.ini')
    config.read(config_path)

    logger.info("====== Starting 2-Stage Filtering Agent ======")

    # --- Setup Source DB Session ---
    source_engine = create_engine(config.get("database", "url"))
    SourceSession = sessionmaker(bind=source_engine)
    source_session = SourceSession()

    try:
        # --- Stage 1 ---
        try:
            stage1_results = get_stage1_symbols(config, source_session)
            stage1_symbols = {s.symbol for s in stage1_results}
            logger.info(f"Stage 1 found {len(stage1_symbols)} symbols.")
        except OperationalError as e:
            if "no such table" in str(e).lower():
                logger.error("Table 'symbols' not found in base_symbols.db. Please run streaming_agent.py first.")
                return
            else: raise

        # --- Stage 2 ---
        try:
            stage2_symbols = get_stage2_symbols(config)
        except OperationalError as e:
            if "no such table" in str(e).lower():
                logger.error("Table 'exchange_info' not found. Please run exchangeinfo_agent.py first.")
                return
            else: raise
        
        # --- Intersection ---
        final_symbol_names = stage1_symbols.intersection(stage2_symbols)
        logger.info(f"Intersection: {len(final_symbol_names)} symbols passed both stages.")
        
        final_symbols_to_store = [s for s in stage1_results if s.symbol in final_symbol_names]
        
        if final_symbols_to_store:
            logger.info("Final symbols to be stored: " + ", ".join([s.symbol for s in final_symbols_to_store[:20]]) + ("..." if len(final_symbols_to_store) > 20 else ""))
        else:
            logger.info("No symbols passed all filter stages.")

        # --- Storage ---
        dest_engine = create_engine(DEST_DB_URL)
        BaseSymbolsBase.metadata.create_all(dest_engine)
        DestSession = sessionmaker(bind=dest_engine)
        dest_session = DestSession()
        
        try:
            dest_session.query(Symbol).delete()
            if final_symbols_to_store:
                for symbol in final_symbols_to_store:
                    source_session.expunge(symbol)
                    dest_session.merge(symbol)
            dest_session.commit()
            logger.info(f"Successfully stored {len(final_symbols_to_store)} final symbols into {DEST_DB_URL}")
        finally:
            dest_session.close()

    except Exception as e:
        logger.error(f"An unexpected error occurred in the agent: {e}", exc_info=True)
    finally:
        source_session.close()
        logger.info("====== Filtering Agent Finished ======")

if __name__ == '__main__':
    run_filtering_agent()
