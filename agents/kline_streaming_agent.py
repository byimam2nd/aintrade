
import asyncio
import json
import logging
import configparser
import os
from typing import Dict, Any, List
from datetime import datetime # Added for the logging message

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import OperationalError
import websockets

from core.base_agent import BaseAgent
from models.base_symbols_models import Symbol as FilteredSymbol
from models.dynamic_models import create_kline_model

# --- Global base for dynamic models (managed by agent instance) ---
# Each model needs a unique base to avoid metadata conflicts if multiple agents are run
# Base = declarative_base() # This was problematic in the original, will be managed within agent

class KlineStreamingAgent(BaseAgent):
    def __init__(self, agent_id: str, config: Dict[str, Any] = None):
        super().__init__(agent_id, config)
        self.config_parser = configparser.ConfigParser()
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_path = os.path.join(project_root, 'config', 'config.ini')
        self.config_parser.read(config_path)
        self.logger.info(f"Loaded config from {config_path}")

        # --- Configuration ---
        self.SOURCE_DB_URL = self.config_parser.get('kline_streaming_agent', 'source_db_url', fallback='sqlite:///database/filtered_tradable_symbols.db')
        self.HISTORICAL_DB_DIR = self.config_parser.get('kline_streaming_agent', 'historical_db_dir', fallback='database/historical_filtered_symbols')
        self.TIME_FRAMES = json.loads(self.config_parser.get('kline_streaming_agent', 'time_frames', fallback='["15m", "1h", "4h"]'))
        self.BASE_STREAM_URL = self.config_parser.get('kline_streaming_agent', 'base_stream_url', fallback='wss://stream.binance.com:9443/stream?streams=')

        # --- Instance cache for database engines and models ---
        self.db_engines: Dict[str, Any] = {}
        self.kline_models: Dict[str, Any] = {}
        self.kline_base = declarative_base() # Base for dynamic models specific to this agent instance

    async def process(self, input_data: Any = None) -> Dict[str, Any]:
        """Main function to set up and run the streaming agent."""
        self.logger.info("====== Starting Real-time K-line Streaming Agent ======")
        
        # 1. Get the list of symbols to process
        symbols = await self._get_symbols_to_stream()
        if not symbols:
            self.logger.warning("No symbols to stream. Exiting process.")
            return {"status": "failure", "message": "No symbols to stream"}

        # 2. Construct the stream URL
        stream_url = self._construct_stream_url(symbols)
        
        # Run the streaming client
        try:
            await self._connect_and_stream(stream_url)
            return {"status": "success", "message": "Streaming agent stopped normally."}
        except KeyboardInterrupt:
            self.logger.info("Streaming stopped by user.")
            return {"status": "interrupted", "message": "Streaming stopped by user."}
        except Exception as e:
            self.logger.error(f"Error running streaming agent: {e}", exc_info=True)
            return {"status": "error", "message": str(e)}

    async def _get_symbols_to_stream(self) -> List[str]:
        source_engine = await asyncio.to_thread(create_engine, self.SOURCE_DB_URL)
        SourceSession = await asyncio.to_thread(sessionmaker, bind=source_engine)
        session = await asyncio.to_thread(SourceSession)
        symbols = []
        try:
            symbols = [s.symbol.lower() for s in await asyncio.to_thread(session.query(FilteredSymbol).all)]
            if not symbols:
                self.logger.warning(f"No symbols found in '{self.SOURCE_DB_URL}'.")
            else:
                self.logger.info(f"Found {len(symbols)} symbols to stream.")
        except OperationalError:
            self.logger.error(f"Could not read from '{self.SOURCE_DB_URL}'. Ensure filtering_agent.py has run.")
        finally:
            await asyncio.to_thread(session.close)
        return symbols

    def _construct_stream_url(self, symbols: List[str]) -> str:
        streams = [f"{symbol}@kline_{tf}" for symbol in symbols for tf in self.TIME_FRAMES]
        return self.BASE_STREAM_URL + "/".join(streams)

    def _get_db_engine(self, symbol):
        if symbol not in self.db_engines:
            db_path = os.path.join(self.HISTORICAL_DB_DIR, f'{symbol}.db')
            # Ensure the directory exists before creating the engine
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
            self.db_engines[symbol] = create_engine(f'sqlite:///{db_path}')
        return self.db_engines[symbol]

    def _get_kline_model(self, symbol, timeframe):
        key = f"{symbol}_{timeframe}"
        if key not in self.kline_models:
            self.kline_models[key] = create_kline_model(self.kline_base, timeframe)
            # Create tables for this specific model
            # This needs to be done in an async way if called within an async context
            # For now, assuming it's okay for initial table creation or will be handled by orchestrator setup
            engine = self._get_db_engine(symbol)
            self.kline_base.metadata.create_all(engine)
        return self.kline_models[key]

        async def _handle_kline_message(self, msg):

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

                

                self.logger.debug(f"Received closed kline for {symbol} [{interval}]")

    

                engine = self._get_db_engine(symbol)

                KlineModel = self._get_kline_model(symbol, interval)

                

                Session = await asyncio.to_thread(sessionmaker, bind=engine)

                session = await asyncio.to_thread(Session)

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

                    await asyncio.to_thread(session.merge, kline_obj)

                    await asyncio.to_thread(session.commit)

                    self.logger.info(f"Updated kline for {symbol} [{interval}] at {datetime.fromtimestamp(kline['t']/1000)}")

                except Exception as e:

                    self.logger.error(f"DB Error processing {symbol} [{interval}]: {e}")

                    await asyncio.to_thread(session.rollback)

                finally:

                    await asyncio.to_thread(session.close)

    

            except json.JSONDecodeError:

                self.logger.warning(f"Could not decode JSON from message: {msg}")

            except Exception as e:

                self.logger.error(f"Error in _handle_kline_message: {e}", exc_info=True)

    

    

        async def _connect_and_stream(self, stream_url):

            """Connects to the WebSocket and processes messages."""

            self.logger.info(f"Connecting to {stream_url.count('@kline_') + stream_url.count('@depth') if stream_url else 0} kline streams...")

            while True:

                try:

                    async with websockets.connect(stream_url, ping_interval=60, ping_timeout=20) as ws:

                        self.logger.info("WebSocket connected successfully.")

                        while True:

                            message = await ws.recv()

                            await self._handle_kline_message(message)

                except websockets.ConnectionClosed:

                    self.logger.warning("WebSocket disconnected, reconnecting...")

                except Exception as e:

                    self.logger.error(f"WebSocket connection error: {e}")

                

                self.logger.info("Retrying in 10 seconds...")

                await asyncio.sleep(10)