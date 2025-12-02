
import requests
import configparser
import os
import asyncio
from typing import Dict, Any

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.base_agent import BaseAgent
from models.exchangeinfo_models import Base, ExchangeInfo

class ExchangeInfoAgent(BaseAgent):
    def __init__(self, agent_id: str, config: Dict[str, Any] = None):
        super().__init__(agent_id, config)
        self.config_parser = configparser.ConfigParser()
        # Path to the main config.ini, assuming it's in the project root's config folder
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_path = os.path.join(project_root, 'config', 'config.ini')
        self.config_parser.read(config_path)
        self.logger.info(f"Loaded config from {config_path}")

    async def process(self, input_data: Any = None) -> Dict[str, Any]:
        self.logger.info("--- Starting ExchangeInfo Agent ---")
        exchange_data = await self._fetch_exchange_info()
        if exchange_data:
            await self._store_exchange_info(exchange_data)
        self.logger.info("--- ExchangeInfo Agent Finished ---")
        return {"status": "success" if exchange_data else "failure"}

    async def _fetch_exchange_info(self):
        """Fetches exchange information from the Binance API."""
        api_url = self.config_parser.get("binance", "exchange_info_url")
        try:
            self.logger.info(f"Fetching data from {api_url}")
            response = await asyncio.to_thread(requests.get, api_url, timeout=10)
            response.raise_for_status()
            self.logger.info("Data fetched successfully.")
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching data from API: {e}")
            return None

        async def _store_exchange_info(self, data):

            """Stores the fetched exchange information into the database."""

            if not data or 'symbols' not in data:

                self.logger.warning("No symbol data to store.")

                return

    

            db_url = "sqlite:///database/exchangeinfo.db" # As per original request, this agent has its own DB

            quote_asset = self.config_parser.get("binance", "quote_asset")

            

            engine = await asyncio.to_thread(create_engine, db_url)

            await asyncio.to_thread(Base.metadata.create_all, engine)

            Session = await asyncio.to_thread(sessionmaker, bind=engine)

            session = await asyncio.to_thread(Session)

    

            try:

                symbols_data = data['symbols']

                self.logger.info(f"Received {len(symbols_data)} symbols from API.")

    

                # Filter symbols based on quote_asset from config

                if quote_asset != "ALL":

                    symbols_data = [s for s in symbols_data if s.get('quoteAsset') == quote_asset]

                    self.logger.info(f"Filtered down to {len(symbols_data)} symbols for quote asset '{quote_asset}'.")

    

                if not symbols_data:

                    self.logger.warning("No symbols to process after filtering.")

                    return

    

                self.logger.info(f"Processing and storing {len(symbols_data)} symbols...")

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

                    await asyncio.to_thread(session.merge, exchange_info_entry)

    

                await asyncio.to_thread(session.commit)

                self.logger.info(f"Successfully stored/updated data for {len(symbols_data)} symbols.")

    

            except Exception as e:

                self.logger.error(f"Database error: {e}")

                await asyncio.to_thread(session.rollback)

            finally:

                await asyncio.to_thread(session.close)
