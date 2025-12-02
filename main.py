
import logging
import time
import threading

# Import the main functions from each agent
from agents import kline_streaming_agent
from agents import Teknikal_Analisys_Agent

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('LiveOrchestrator')

def run_live_phase():
    """Runs the continuous live trading agents."""
    logger.info("====== STARTING LIVE TRADING PHASE ======")
    
    # 1. Run the kline streaming agent in a separate thread
    logger.info("--- Starting Real-time K-line Streaming Agent (in background) ---")
    kline_streamer_thread = threading.Thread(target=kline_streaming_agent.main, name="KlineStreamer")
    kline_streamer_thread.daemon = True
    kline_streamer_thread.start()
    
    # Give the streamer a moment to connect and receive initial data
    logger.info("Waiting 10 seconds for K-line streamer to establish connection...")
    time.sleep(10)
    
    # 2. Run the analysis agent in a continuous loop
    logger.info("--- Starting continuous analysis loop ---")
    while True:
        try:
            Teknikal_Analisys_Agent.run_analysis_agent()
            logger.debug("Analysis cycle complete.")
        except Exception as e:
            logger.error(f"An error occurred during the analysis loop: {e}", exc_info=True)
            logger.info("Attempting to continue after 10 seconds...")
            time.sleep(10) # Small sleep to prevent tight looping on persistent errors

if __name__ == '__main__':
    try:
        run_live_phase()
    except KeyboardInterrupt:
        logger.info("Live trading stopped by user. Exiting.")
