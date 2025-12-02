import asyncio
import logging
import pandas as pd # May be needed if strategies rely on pd.Series/DataFrame for signals
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import OperationalError
import os
import importlib
import inspect
from datetime import datetime
from typing import Dict, Any, List, Tuple

from core.base_agent import BaseAgent
from models.signals_models import Base as SignalsBase, Signal

class SignalAgent(BaseAgent):
    def __init__(self, agent_id: str, config: Dict[str, Any] = None):
        super().__init__(agent_id, config)
        self.config_parser = configparser.ConfigParser()
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_path = os.path.join(project_root, 'config', 'config.ini')
        self.config_parser.read(config_path)
        self.logger.info(f"Loaded config from {config_path}")

        # --- Configuration ---
        self.SIGNALS_DB_URL = self.config_parser.get('signal_agent', 'signals_db_url', fallback='sqlite:///database/signals.db')
        self.STRATEGIES_DIR = self.config_parser.get('signal_agent', 'strategies_dir', fallback='strategies')

        self.signals_engine = None
        self.strategies = [] # To hold instantiated strategy objects

    async def process(self, input_data: Dict[str, Dict[str, pd.DataFrame]]) -> Dict[str, Any]:
        self.logger.info("====== Starting Signal Agent Cycle ======")
        
        # Load strategies (needed to call get_signal)
        await self._load_strategies()
        if not self.strategies:
            self.logger.warning("No strategies found. Exiting cycle.")
            return {"status": "failure", "message": "No strategies found"}

        # Initialize Signals DB
        self.signals_engine = await asyncio.to_thread(create_engine, self.SIGNALS_DB_URL)
        await asyncio.to_thread(SignalsBase.metadata.create_all, self.signals_engine)

        if not input_data:
            self.logger.warning("No enriched data received for signal generation. Exiting cycle.")
            return {"status": "failure", "message": "No input data"}
        
        self.logger.info(f"Generating signals for {len(input_data)} symbols...")

        generated_signals = []
        for symbol, enriched_data_for_symbol in input_data.items():
            signals = await self._generate_signals_for_symbol(symbol, enriched_data_for_symbol)
            generated_signals.extend(signals)
        
        self.logger.info(f"Total {len(generated_signals)} signals generated and stored.")
        self.logger.info("====== Signal Agent Cycle Finished ======")
        return {"status": "success", "total_signals": len(generated_signals), "message": "Signals generated and stored"}

    async def _load_strategies(self):
        """Dynamically loads all strategy classes from the strategies directory."""
        strategy_classes = []
        for filename in os.listdir(self.STRATEGIES_DIR):
            if filename.endswith('_strategy.py'):
                module_name = f"{self.STRATEGIES_DIR}.{filename[:-3]}"
                try:
                    spec = importlib.util.spec_from_file_location(module_name, os.path.join(self.STRATEGIES_DIR, filename))
                    module = importlib.util.module_from_spec(spec)
                    await asyncio.to_thread(spec.loader.exec_module, module)
                    
                    for name, obj in inspect.getmembers(module):
                        if inspect.isclass(obj) and hasattr(obj, 'get_signal') and hasattr(obj, 'prepare_data'): # Check prepare_data for strategy
                            strategy_classes.append(obj)
                            self.logger.info(f"Successfully loaded strategy: {name}")
                except (ImportError, AttributeError, FileNotFoundError) as e:
                    self.logger.error(f"Failed to import strategy from {filename}: {e}")
        self.strategies = [Strategy() for Strategy in strategy_classes]

    async def _generate_signals_for_symbol(self, symbol: str, enriched_data: Dict[str, pd.DataFrame]) -> List[Dict[str, Any]]:
        symbol_signals = []
        for strategy in self.strategies:
            if not all(tf in enriched_data for tf in strategy.timeframes):
                self.logger.warning(f"[{symbol}] Missing required timeframes for strategy '{strategy.name}'. Skipping signal generation.")
                continue
            
            # Prepare latest candles for signal generation
            latest_candles = {}
            for tf in strategy.timeframes:
                if not enriched_data[tf].empty:
                    latest_candles[tf] = enriched_data[tf].iloc[-1]
            
            if len(latest_candles) != len(strategy.timeframes):
                self.logger.warning(f"[{symbol}] Not all latest candle data present for strategy '{strategy.name}'. Skipping signal generation.")
                continue

            signal, timeframe, kline_time = await asyncio.to_thread(strategy.get_signal, symbol, latest_candles)

            if signal != 'HOLD':
                await self._store_signal(symbol, signal, strategy.name, timeframe, kline_time)
                symbol_signals.append({
                    "symbol": symbol,
                    "signal": signal,
                    "strategy": strategy.name,
                    "timeframe": timeframe,
                    "kline_time": kline_time
                })
        return symbol_signals

    async def _store_signal(self, symbol: str, signal: str, strategy_name: str, timeframe: str, kline_time: int):
        SignalsSession = await asyncio.to_thread(sessionmaker, bind=self.signals_engine)
        signals_session = await asyncio.to_thread(SignalsSession)
        try:
            # Check for existing signal
            existing_signal = await asyncio.to_thread(
                signals_session.query(Signal).filter_by,
                symbol=symbol, strategy=strategy_name, timeframe=timeframe, kline_open_time=kline_time
            )
            existing_signal = await asyncio.to_thread(existing_signal.first)

            if not existing_signal:
                new_signal = Signal(
                    symbol=symbol, signal=signal, strategy=strategy_name,
                    timeframe=timeframe, kline_open_time=kline_time
                )
                await asyncio.to_thread(signals_session.add, new_signal)
                await asyncio.to_thread(signals_session.commit)
                self.logger.info(f"Stored new {signal} signal for {symbol} from strategy '{strategy.name}'.")
        except Exception as e:
            self.logger.error(f"Error storing signal for {symbol} - {strategy_name}: {e}", exc_info=True)
            await asyncio.to_thread(signals_session.rollback)
        finally:
            await asyncio.to_thread(signals_session.close)
