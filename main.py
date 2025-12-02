
import asyncio
import logging
import time 

from core.orchestrator import AgentOrchestrator
from agents.exchangeinfo_agent import ExchangeInfoAgent
from agents.kline_streaming_agent import KlineStreamingAgent
from agents.indicator_agent import IndicatorAgent
from agents.signal_agent import SignalAgent


logger = logging.getLogger('MainOrchestrator')

async def main_orchestrator():
    logger.info("====== Initializing and Orchestrating Agents ======")

    orchestrator = AgentOrchestrator(config={"live_mode": True}) # Example config

    # Initialize and register agents
    exchange_info_agent = ExchangeInfoAgent("ExchangeInfoAgent")
    kline_streaming_agent = KlineStreamingAgent("KlineStreamingAgent")
    indicator_agent = IndicatorAgent("IndicatorAgent")
    signal_agent = SignalAgent("SignalAgent")

    orchestrator.register_agent(exchange_info_agent)
    orchestrator.register_agent(kline_streaming_agent)
    orchestrator.register_agent(indicator_agent)
    orchestrator.register_agent(signal_agent)

    # --- Initial Setup Phase ---
    logger.info("--- Running ExchangeInfo Agent (once) ---")
    await orchestrator.execute_workflow([
        {"agent_id": "ExchangeInfoAgent", "input_data": None}
    ])
    logger.info("ExchangeInfo Agent finished initial run.")

    # --- Live Phase ---
    logger.info("--- Starting Real-time K-line Streaming Agent (in background task) ---")
    kline_task = asyncio.create_task(kline_streaming_agent.process())
    
    logger.info("Waiting 10 seconds for K-line streamer to establish connection...")
    await asyncio.sleep(10)
    
    logger.info("--- Starting continuous Analysis and Signal Generation loop ---")
    analysis_interval_seconds = 60 # Run analysis every 60 seconds, or configurable
    while True:
        try:
            # 1. Run Indicator Agent
            indicator_results = await orchestrator.execute_workflow([
                {"agent_id": "IndicatorAgent", "input_data": None}
            ])
            
            # Extract data from indicator results to pass to signal agent
            # Assuming indicator_results will have a structure like {"IndicatorAgent": {"status": "success", "data": {symbols_data}}}
            enriched_data = indicator_results.get("IndicatorAgent", {}).get("data")

            if enriched_data:
                # 2. Run Signal Agent
                await orchestrator.execute_workflow([
                    {"agent_id": "SignalAgent", "input_data": enriched_data}
                ])
                logger.debug(f"Analysis and Signal Generation cycle complete. Waiting {analysis_interval_seconds} seconds.")
            else:
                logger.warning("No enriched data from Indicator Agent. Skipping Signal Agent run.")

        except Exception as e:
            logger.error(f"An error occurred during the analysis and signal generation loop: {e}", exc_info=True)
            logger.info("Attempting to continue after 10 seconds...")
            await asyncio.sleep(10)

        await asyncio.sleep(analysis_interval_seconds)

if __name__ == '__main__':
    try:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        asyncio.run(main_orchestrator())
    except KeyboardInterrupt:
        logger.info("Orchestration stopped by user. Exiting.")
    except Exception as e:
        logger.critical(f"An unhandled error occurred in the main orchestrator: {e}", exc_info=True)
