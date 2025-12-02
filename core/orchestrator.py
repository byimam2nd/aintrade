from typing import Dict, Any, List
from core.base_agent import BaseAgent
import logging

class AgentOrchestrator:
    """
    Manages and orchestrates the execution of various agents in a defined workflow.
    """
    def __init__(self, config: Dict[str, Any] = None):
        self.agents: Dict[str, BaseAgent] = {}
        self.config = config if config is not None else {}
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("AgentOrchestrator initialized.")

    def register_agent(self, agent: BaseAgent):
        """
        Registers an agent with the orchestrator.
        """
        if agent.agent_id in self.agents:
            self.logger.warning(f"Agent '{agent.agent_id}' already registered. Overwriting.")
        self.agents[agent.agent_id] = agent
        self.logger.info(f"Agent '{agent.agent_id}' registered.")

    async def execute_workflow(self, workflow_steps: List[Dict[str, Any]]):
        """
        Executes a sequence of workflow steps.
        Each step should specify an 'agent_id' and 'input_data'.
        """
        self.logger.info(f"Starting workflow execution with {len(workflow_steps)} steps.")
        results = {}
        for i, step in enumerate(workflow_steps):
            agent_id = step.get("agent_id")
            input_data = step.get("input_data")
            
            if agent_id not in self.agents:
                self.logger.error(f"Workflow step {i+1}: Agent '{agent_id}' not found. Skipping step.")
                continue

            agent = self.agents[agent_id]
            self.logger.info(f"Workflow step {i+1}: Executing agent '{agent_id}' with input: {input_data}")
            try:
                # In a real scenario, input_data might reference previous results or be more complex
                step_result = await agent.process(input_data)
                results[agent_id] = step_result # Store result, might be useful for subsequent steps
                self.logger.info(f"Workflow step {i+1}: Agent '{agent_id}' completed. Result: {step_result}")
            except Exception as e:
                self.logger.error(f"Workflow step {i+1}: Agent '{agent_id}' failed with error: {e}")
                # Depending on the workflow, you might want to stop or continue
        self.logger.info("Workflow execution finished.")
        return results

    def get_agent(self, agent_id: str) -> BaseAgent:
        """
        Retrieves a registered agent by its ID.
        """
        return self.agents.get(agent_id)
