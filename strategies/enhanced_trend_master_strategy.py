import logging
import pandas as pd
import numpy as np
import ta
from datetime import datetime

logger = logging.getLogger(__name__)

class EnhancedTrendMasterStrategy:
    """
    A class that implements the 'enhanced_trend_master' strategy.
    It separates data preparation from signal analysis for efficiency.
    """
    
    def __init__(self):
        self.name = 'enhanced_trend_master'
        self.timeframes = ["15m", "1h", "4h"]

    def _calculate_supertrend(self, df: pd.DataFrame, atr_period: int = 10, multiplier: float = 3.0):
        df['atr'] = ta.volatility.average_true_range(df['high'], df['low'], df['close'], window=atr_period)
        df['upper_band'] = ((df['high'] + df['low']) / 2) + (multiplier * df['atr'])
        df['lower_band'] = ((df['high'] + df['low']) / 2) - (multiplier * df['atr'])
        df['in_uptrend'] = True

        for current in range(1, len(df.index)):
            previous = current - 1
            if df.loc[current, 'close'] > df.loc[previous, 'upper_band']:
                df.loc[current, 'in_uptrend'] = True
            elif df.loc[current, 'close'] < df.loc[previous, 'lower_band']:
                df.loc[current, 'in_uptrend'] = False
            else:
                df.loc[current, 'in_uptrend'] = df.loc[previous, 'in_uptrend']
                if df.loc[current, 'in_uptrend'] and df.loc[current, 'lower_band'] < df.loc[previous, 'lower_band']:
                    df.loc[current, 'lower_band'] = df.loc[previous, 'lower_band']
                if not df.loc[current, 'in_uptrend'] and df.loc[current, 'upper_band'] > df.loc[previous, 'upper_band']:
                    df.loc[current, 'upper_band'] = df.loc[previous, 'upper_band']

        df['supertrend'] = np.where(df['in_uptrend'], df['lower_band'], df['upper_band'])
        df['supertrend_direction'] = np.where(df['in_uptrend'], 1, -1)
        return df

    def prepare_data(self, kline_data: dict):
        """
        Calculates all necessary indicators for the strategy on the given data.
        This method should be called once before running analysis in a loop.
        """
        for tf in self.timeframes:
            if tf not in kline_data: continue
            df = kline_data[tf]
            df['close'] = pd.to_numeric(df['close'])
            df['high'] = pd.to_numeric(df['high'])
            df['low'] = pd.to_numeric(df['low'])
            df['open'] = pd.to_numeric(df['open'])
            df['volume'] = pd.to_numeric(df['volume'])
            df['ema9'] = ta.trend.ema_indicator(df['close'], window=9)
            df['ema20'] = ta.trend.ema_indicator(df['close'], window=20)
            df['ema50'] = ta.trend.ema_indicator(df['close'], window=50)
            df = self._calculate_supertrend(df)
            df['rsi14'] = ta.momentum.rsi(df['close'], window=14)
            df['volume_avg20'] = ta.volume.chaikin_money_flow(df['high'], df['low'], df['close'], df['volume'], window=20)
            df['typical_price'] = (df['high'] + df['low'] + df['close']) / 3
            df['vwap_approx'] = df['typical_price'].rolling(window=20).mean()
            kline_data[tf] = df
        return kline_data

    def get_signal(self, symbol: str, latest_candles: dict):
        """
        Analyzes the latest candle data (with pre-calculated indicators)
        to generate a trading signal.

        Args:
            symbol (str): The symbol being analyzed.
            latest_candles (dict): A dictionary where keys are timeframes and
                                   values are pandas Series representing the
                                   latest candle for that timeframe.
        """
        primary_tf = "4h"
        confirmation_tf = "1h"
        entry_tf = "15m"

        try:
            latest_4h = latest_candles[primary_tf]
            latest_1h = latest_candles[confirmation_tf]
            latest_15m = latest_candles[entry_tf]
        except KeyError as e:
            logger.debug(f"[{symbol}/{self.name}] Not all latest candle data available: {e}")
            return 'HOLD', None, None

        # --- BUY Conditions ---
        ema_bullish = (latest_4h['ema9'] > latest_4h['ema20'] > latest_4h['ema50']) and \
                      (latest_1h['ema9'] > latest_1h['ema20'] > latest_1h['ema50'])
        supertrend_bullish = (latest_4h['supertrend_direction'] == 1) and \
                             (latest_1h['supertrend_direction'] == 1) and \
                             (latest_15m['supertrend_direction'] == 1)
        rsi_ok_buy = (latest_15m['rsi14'] >= 45 and latest_15m['rsi14'] <= 65)
        volume_ok_buy = (latest_15m['volume_avg20'] > 0.1)
        vwap_ok_buy = (latest_15m['close'] > latest_15m['vwap_approx'])

        if ema_bullish and supertrend_bullish and rsi_ok_buy and volume_ok_buy and vwap_ok_buy:
            kline_open_time = latest_15m['open_time']
            return 'BUY', entry_tf, kline_open_time

        # --- SELL Conditions ---
        ema_bearish = (latest_4h['ema9'] < latest_4h['ema20'] < latest_4h['ema50']) and \
                      (latest_1h['ema9'] < latest_1h['ema20'] < latest_1h['ema50'])
        supertrend_bearish = (latest_4h['supertrend_direction'] == -1) and \
                             (latest_1h['supertrend_direction'] == -1) and \
                             (latest_15m['supertrend_direction'] == -1)
        rsi_ok_sell = (latest_15m['rsi14'] >= 35 and latest_15m['rsi14'] <= 55)
        volume_ok_sell = (latest_15m['volume_avg20'] < -0.1)
        vwap_ok_sell = (latest_15m['close'] < latest_15m['vwap_approx'])

        if ema_bearish and supertrend_bearish and rsi_ok_sell and volume_ok_sell and vwap_ok_sell:
            kline_open_time = latest_15m['open_time']
            return 'SELL', entry_tf, kline_open_time
        
        return 'HOLD', None, None
