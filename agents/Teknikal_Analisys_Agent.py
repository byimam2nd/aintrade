
import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import OperationalError
import sys
import os
import importlib
import inspect
from datetime import datetime

# Add the root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.base_symbols_models import Symbol as FilteredSymbol
from models.signals_models import Base as SignalsBase, Signal
from models.dynamic_models import create_kline_model

# --- Configuration ---
SOURCE_DB_URL = 'sqlite:///database/filtered_tradable_symbols.db'
HISTORICAL_DB_DIR = 'database/historical_filtered_symbols'
SIGNALS_DB_URL = 'sqlite:///database/signals.db'
STRATEGIES_DIR = 'strategies'

# --- Logging Setup ---
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.propagate = False
if not logger.handlers:
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

def load_strategies():
    """Dynamically loads all strategy classes from the strategies directory."""
    strategy_classes = []
    for filename in os.listdir(STRATEGIES_DIR):
        if filename.endswith('_strategy.py'):
            module_name = f"{STRATEGIES_DIR}.{filename[:-3]}"
            try:
                module = importlib.import_module(module_name)
                for name, obj in inspect.getmembers(module):
                    if inspect.isclass(obj) and hasattr(obj, 'get_signal') and hasattr(obj, 'prepare_data'):
                        strategy_classes.append(obj)
                        logger.info(f"Successfully loaded strategy: {name}")
            except ImportError as e:
                logger.error(f"Failed to import strategy from {filename}: {e}")
    return strategy_classes

def run_analysis_agent():
    """
    Main agent to run all technical analysis strategies on historical data.
    """
    logger.info("====== Starting Technical Analysis Agent Cycle ======")
    
    strategy_classes = load_strategies()
    if not strategy_classes:
        logger.warning("No strategies found. Exiting cycle.")
        return
    strategies = [Strategy() for Strategy in strategy_classes]
    
    signals_engine = create_engine(SIGNALS_DB_URL)
    SignalsBase.metadata.create_all(signals_engine)
    SignalsSession = sessionmaker(bind=signals_engine)

    source_engine = create_engine(SOURCE_DB_URL)
    SourceSession = sessionmaker(bind=source_engine)
    source_session = SourceSession()
    try:
        symbols = [s.symbol for s in source_session.query(FilteredSymbol).all()]
    finally:
        source_session.close()
    
    if not symbols:
        logger.warning(f"No symbols found in '{SOURCE_DB_URL}' to analyze.")
        return
    logger.info(f"Analyzing {len(symbols)} symbols...")

    for symbol in symbols:
        try:
            all_required_tfs = set()
            for s in strategies:
                all_required_tfs.update(s.timeframes)

            db_path = os.path.join(HISTORICAL_DB_DIR, f'{symbol}.db')
            if not os.path.exists(db_path):
                logger.warning(f"No historical database for {symbol}. Skipping.")
                continue
            
            symbol_engine = create_engine(f'sqlite:///{db_path}')
            SymbolSession = sessionmaker(bind=symbol_engine)
            symbol_session = SymbolSession()
            
            kline_data = {}
            SymbolBase = declarative_base()
            for tf in all_required_tfs:
                KlineModel = create_kline_model(SymbolBase, tf)
                query = symbol_session.query(KlineModel).order_by(KlineModel.open_time.asc())
                df = pd.read_sql(query.statement, symbol_session.bind)
                if not df.empty:
                    kline_data[tf] = df
            symbol_session.close()

            for strategy in strategies:
                if not all(tf in kline_data for tf in strategy.timeframes):
                    logger.warning(f"[{symbol}] Missing required timeframes for strategy '{strategy.name}'. Skipping.")
                    continue

                # Prepare data (calculates indicators)
                kline_data_with_indicators = strategy.prepare_data(kline_data.copy())

                # Prepare latest candles for signal generation
                latest_candles = {}
                for tf in strategy.timeframes:
                    if not kline_data_with_indicators[tf].empty:
                        latest_candles[tf] = kline_data_with_indicators[tf].iloc[-1]
                
                if len(latest_candles) != len(strategy.timeframes):
                    continue # Not all data was present

                signal, timeframe, kline_time = strategy.get_signal(symbol, latest_candles)

                if signal != 'HOLD':
                    signals_session = SignalsSession()
                    try:
                        existing_signal = signals_session.query(Signal).filter_by(
                            symbol=symbol, strategy=strategy.name, timeframe=timeframe, kline_open_time=kline_time
                        ).first()

                        if not existing_signal:
                            new_signal = Signal(
                                symbol=symbol, signal=signal, strategy=strategy.name,
                                timeframe=timeframe, kline_open_time=kline_time
                            )
                            signals_session.add(new_signal)
                            signals_session.commit()
                            logger.info(f"Stored new {signal} signal for {symbol} from strategy '{strategy.name}'.")
                    finally:
                        signals_session.close()

        except Exception as e:
            logger.error(f"Failed to process symbol {symbol}: {e}", exc_info=True)

    logger.info("====== Technical Analysis Agent Cycle Finished ======")

if __name__ == '__main__':
    run_analysis_agent()
