
import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
import sys
import os
import argparse

# Add the root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.dynamic_models import create_kline_model
from strategies.enhanced_trend_master_strategy import EnhancedTrendMasterStrategy

# --- Configuration ---
HISTORICAL_DB_DIR = 'database/historical_filtered_symbols'

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('BacktestAgent')

def run_backtest(symbol: str, starting_capital: float = 1000.0):
    """
    Runs an efficient, stateful backtest for a given symbol.
    """
    logger.info(f"====== Starting Backtest for {symbol} with ${starting_capital} ======")
    
    strategy = EnhancedTrendMasterStrategy()

    # 1. Load all historical data
    db_path = os.path.join(HISTORICAL_DB_DIR, f'{symbol}.db')
    if not os.path.exists(db_path):
        logger.error(f"No historical database for {symbol}. Exiting.")
        return

    engine = create_engine(f'sqlite:///{db_path}')
    Session = sessionmaker(bind=engine)
    session = Session()
    
    kline_data_full = {}
    Base = declarative_base()
    try:
        for tf in strategy.timeframes:
            KlineModel = create_kline_model(Base, tf)
            query = session.query(KlineModel).order_by(KlineModel.open_time.asc())
            df = pd.read_sql(query.statement, session.bind)
            if df.empty:
                logger.error(f"No data for timeframe {tf} in {symbol}. Exiting.")
                return
            df['timestamp'] = pd.to_datetime(df['open_time'], unit='ms')
            kline_data_full[tf] = df
    finally:
        session.close()

    logger.info("Loaded all timeframes. Preparing data and indicators...")

    # 2. Prepare all indicators ONCE for performance
    kline_data_full = strategy.prepare_data(kline_data_full)

    # 3. Align dataframes using merge_asof
    df_15m = kline_data_full['15m'].dropna().add_suffix('_15m')
    df_1h = kline_data_full['1h'].dropna().add_suffix('_1h')
    df_4h = kline_data_full['4h'].dropna().add_suffix('_4h')

    df_15m.rename(columns={'timestamp_15m': 'timestamp'}, inplace=True)
    df_1h.rename(columns={'timestamp_1h': 'timestamp'}, inplace=True)
    df_4h.rename(columns={'timestamp_4h': 'timestamp'}, inplace=True)

    aligned_df = pd.merge_asof(df_15m, df_1h, on='timestamp')
    aligned_df = pd.merge_asof(aligned_df, df_4h, on='timestamp')
    aligned_df.dropna(inplace=True)

    logger.info(f"Data prepared and aligned. Total candles to backtest: {len(aligned_df)}")

    # 4. Initialize Portfolio and run simulation loop
    cash = starting_capital
    asset_held = 0.0
    position = 'OUT' # 'IN' or 'OUT'
    trades = []
    
    for i, row in aligned_df.iterrows():
        # Create a dictionary of the latest candle data for the strategy
        # by selecting columns for each timeframe and renaming them
        row_15m = row[[c for c in aligned_df.columns if c.endswith('_15m')]].rename(lambda x: x.replace('_15m', ''))
        row_1h = row[[c for c in aligned_df.columns if c.endswith('_1h')]].rename(lambda x: x.replace('_1h', ''))
        row_4h = row[[c for c in aligned_df.columns if c.endswith('_4h')]].rename(lambda x: x.replace('_4h', ''))

        latest_candles = {
            '15m': row_15m,
            '1h': row_1h,
            '4h': row_4h
        }
        
        signal, _, _ = strategy.get_signal(symbol, latest_candles)
        
        current_price = row['close_15m'] # Use the 15m close price for trading

        # Execute Trade based on signal AND position state
        if signal == 'BUY' and position == 'OUT':
            position = 'IN'
            asset_held = cash / current_price
            trades.append({'time': row['timestamp'], 'type': 'BUY', 'price': current_price, 'amount': asset_held})
            logger.info(f"[{row['timestamp']}] BUY: {asset_held:.6f} {symbol} at ${current_price:.5f}")
            cash = 0.0
        
        elif signal == 'SELL' and position == 'IN':
            position = 'OUT'
            cash = asset_held * current_price
            trades.append({'time': row['timestamp'], 'type': 'SELL', 'price': current_price, 'value': cash})
            logger.info(f"[{row['timestamp']}] SELL: {asset_held:.6f} {symbol} at ${current_price:.5f} for ${cash:.2f}")
            asset_held = 0.0
            
    # 5. Final Report
    logger.info("====== Backtest Finished ======")
    
    final_value = cash
    if position == 'IN':
        last_price = aligned_df.iloc[-1]['close_15m']
        final_value = asset_held * last_price
        logger.info(f"Ending with an open position. Valuing {asset_held:.6f} {symbol} at last price ${last_price:.5f} = ${final_value:.2f}")

    profit = final_value - starting_capital
    profit_percent = (profit / starting_capital) * 100

    logger.info("--- Backtest Performance Report ---")
    logger.info(f"Symbol: {symbol}")
    logger.info(f"Period: {aligned_df.iloc[0]['timestamp']} to {aligned_df.iloc[-1]['timestamp']}")
    logger.info(f"Starting Capital: ${starting_capital:.2f}")
    logger.info(f"Ending Capital:   ${final_value:.2f}")
    logger.info(f"Profit/Loss:      ${profit:.2f} ({profit_percent:.2f}%)")
    logger.info(f"Total Trades:     {len(trades)}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Aintrade Backtesting Agent")
    parser.add_argument('symbol', type=str, help="The symbol to run the backtest on (e.g., DOGEUSDT).")
    parser.add_argument('--capital', type=float, default=1000.0, help="The starting capital for the backtest.")
    args = parser.parse_args()
    
    run_backtest(args.symbol, args.capital)
