import asyncio
import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import OperationalError
import os
import importlib
import inspect
from typing import Dict, Any, List

from core.base_agent import BaseAgent
from models.base_symbols_models import Symbol as FilteredSymbol
from models.dynamic_models import create_kline_model

class IndicatorAgent(BaseAgent):
    def __init__(self, agent_id: str, config: Dict[str, Any] = None):
        super().__init__(agent_id, config)
        self.config_parser = configparser.ConfigParser()
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_path = os.path.join(project_root, 'config', 'config.ini')
        self.config_parser.read(config_path)
        self.logger.info(f"Loaded config from {config_path}")

        # --- Configuration ---
        self.SOURCE_DB_URL = self.config_parser.get('indicator_agent', 'source_db_url', fallback='sqlite:///database/filtered_tradable_symbols.db')
        self.HISTORICAL_DB_DIR = self.config_parser.get('indicator_agent', 'historical_db_dir', fallback='database/historical_filtered_symbols')
        self.STRATEGIES_DIR = self.config_parser.get('indicator_agent', 'strategies_dir', fallback='strategies')

        self.source_engine = None
        self.strategies = [] # To hold instantiated strategy objects

    async def process(self, input_data: Any = None) -> Dict[str, Any]:
        self.logger.info("====== Starting Indicator Agent Cycle ======")
        
        # Load strategies
        await self._load_strategies()
        if not self.strategies:
            self.logger.warning("No strategies found. Exiting cycle.")
            return {"status": "failure", "message": "No strategies found"}

        # Get symbols to analyze
        symbols = await self._get_symbols_to_analyze()
        if not symbols:
            self.logger.warning(f"No symbols found in '{self.SOURCE_DB_URL}' to analyze. Exiting cycle.")
            return {"status": "failure", "message": "No symbols to analyze"}
        
        self.logger.info(f"Processing indicators for {len(symbols)} symbols...")
        
        all_enriched_data = {}
        for symbol in symbols:
            enriched_data = await self._process_symbol_for_indicators(symbol)
            if enriched_data:
                all_enriched_data[symbol] = enriched_data

        self.logger.info("====== Indicator Agent Cycle Finished ======")
        return {"status": "success", "data": all_enriched_data, "message": f"Processed indicators for {len(all_enriched_data)} symbols"}

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
                        if inspect.isclass(obj) and hasattr(obj, 'get_signal') and hasattr(obj, 'prepare_data'):
                            strategy_classes.append(obj)
                            self.logger.info(f"Successfully loaded strategy: {name}")
                except (ImportError, AttributeError, FileNotFoundError) as e:
                    self.logger.error(f"Failed to import strategy from {filename}: {e}")
        self.strategies = [Strategy() for Strategy in strategy_classes]

    async def _get_symbols_to_analyze(self) -> List[str]:
        self.source_engine = await asyncio.to_thread(create_engine, self.SOURCE_DB_URL)
        SourceSession = await asyncio.to_thread(sessionmaker, bind=self.source_engine)
        source_session = await asyncio.to_thread(SourceSession)
        symbols = []
        try:
            symbols = [s.symbol for s in await asyncio.to_thread(source_session.query(FilteredSymbol).all)]
        finally:
            await asyncio.to_thread(source_session.close)
        return symbols

    async def _process_symbol_for_indicators(self, symbol: str) -> Dict[str, pd.DataFrame]:
        self.logger.info(f"Processing indicators for symbol: {symbol}")
        all_required_tfs = set()
        for s in self.strategies:
            all_required_tfs.update(s.timeframes)

        db_path = os.path.join(self.HISTORICAL_DB_DIR, f'{symbol}.db')
        if not os.path.exists(db_path):
            self.logger.warning(f"No historical database for {symbol}. Skipping.")
            return {}
        
        symbol_engine = await asyncio.to_thread(create_engine, f'sqlite:///{db_path}')
        SymbolSession = await asyncio.to_thread(sessionmaker, bind=symbol_engine)
        symbol_session = await asyncio.to_thread(SymbolSession)
        
        kline_data_dfs = {}
        SymbolBase = declarative_base() 

        try:
            for tf in all_required_tfs:
                KlineModel = create_kline_model(SymbolBase, tf)
                query = symbol_session.query(KlineModel).order_by(KlineModel.open_time.asc())
                df = await asyncio.to_thread(pd.read_sql, str(query.statement.compile(dialect=symbol_engine.dialect)), symbol_session.bind)
                if not df.empty:
                    kline_data_dfs[tf] = df
        except Exception as e:
            self.logger.error(f"Error fetching kline data for {symbol}: {e}", exc_info=True)
        finally:
            await asyncio.to_thread(symbol_session.close)

        if not kline_data_dfs:
            self.logger.warning(f"No kline data found for {symbol} after fetching. Skipping indicator calculation.")
            return {}

        enriched_data_per_tf = {}
        for strategy in self.strategies:
            if not all(tf in kline_data_dfs for tf in strategy.timeframes):
                self.logger.warning(f"[{symbol}] Missing required timeframes for strategy '{strategy.name}'. Skipping indicator calculation for this strategy.")
                continue
            
            # Prepare data (calculates indicators)
            # This method should return the DataFrame with indicators added
            kline_data_with_indicators = await asyncio.to_thread(strategy.prepare_data, kline_data_dfs.copy())
            # Store the enriched data per timeframe for this symbol
            for tf in strategy.timeframes:
                enriched_data_per_tf[tf] = kline_data_with_indicators[tf]
        
        return enriched_data_per_tf
