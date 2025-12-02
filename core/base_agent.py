from abc import ABC, abstractmethod
from typing import Dict, Any
import logging

# Configure logging for agents
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class BaseAgent(ABC):
    """
    Base class for all agents in the system.
    Provides a common interface and basic functionalities like logging.
    """
    def __init__(self, agent_id: str, config: Dict[str, Any] = None):
        self.agent_id = agent_id
        self.config = config if config is not None else {}
        self.logger = logging.getLogger(self.agent_id)
        self.logger.info(f"Agent '{self.agent_id}' initialized with config: {self.config}")

    @abstractmethod
    async def process(self, input_data: Any) -> Any:
        """
        Abstract method to be implemented by concrete agents for processing data.
        """
        pass

    def get_config_value(self, key: str, default: Any = None) -> Any:
        """
        Retrieves a configuration value for the agent.
        """
        return self.config.get(key, default)
