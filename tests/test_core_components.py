import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from core.base_agent import BaseAgent
from core.orchestrator import AgentOrchestrator
from typing import Any, Dict

# Mock Agent for testing Orchestrator
class MockAgent(BaseAgent):
    def __init__(self, agent_id: str, config: Dict[str, Any] = None):
        super().__init__(agent_id, config)
        # Store a real AsyncMock for process that can be configured per test
        self._mock_process_impl = AsyncMock(return_value={"result": f"{agent_id} processed"})

    async def process(self, input_data: Any) -> Any:
        # This will call the underlying AsyncMock
        return await self._mock_process_impl(input_data)

    # For easier testing, allow direct access to the mock
    @property
    def mock_process(self):
        return self._mock_process_impl

# Test BaseAgent
def test_base_agent_initialization():
    agent = MockAgent("TestAgent", {"key": "value"})
    assert agent.agent_id == "TestAgent"
    assert agent.config == {"key": "value"}
    assert agent.logger.name == "TestAgent"
    # Ensure process method is an AsyncMock
    assert isinstance(agent.mock_process, AsyncMock)

# Test AgentOrchestrator
@pytest.mark.asyncio
async def test_orchestrator_register_agent():
    orchestrator = AgentOrchestrator()
    mock_agent = MockAgent("MockAgent")
    orchestrator.register_agent(mock_agent)
    assert orchestrator.get_agent("MockAgent") == mock_agent

@pytest.mark.asyncio
async def test_orchestrator_execute_workflow_success():
    orchestrator = AgentOrchestrator()
    mock_agent1 = MockAgent("Agent1")
    mock_agent2 = MockAgent("Agent2")
    orchestrator.register_agent(mock_agent1)
    orchestrator.register_agent(mock_agent2)

    workflow = [
        {"agent_id": "Agent1", "input_data": "data1"},
        {"agent_id": "Agent2", "input_data": "data2"},
    ]

    results = await orchestrator.execute_workflow(workflow)

    mock_agent1.mock_process.assert_called_once_with("data1")
    mock_agent2.mock_process.assert_called_once_with("data2")
    
    assert results == {
        "Agent1": {"result": "Agent1 processed"},
        "Agent2": {"result": "Agent2 processed"}
    }

@pytest.mark.asyncio
async def test_orchestrator_execute_workflow_agent_not_found():
    orchestrator = AgentOrchestrator()
    mock_agent = MockAgent("Agent1")
    orchestrator.register_agent(mock_agent)

    workflow = [
        {"agent_id": "Agent1", "input_data": "data1"},
        {"agent_id": "NonExistentAgent", "input_data": "dataX"},
    ]
    
    # We expect an error log, but the workflow should continue or return partial results
    # For this test, we'll check that Agent1 was called and NonExistentAgent was skipped
    results = await orchestrator.execute_workflow(workflow)

    mock_agent.mock_process.assert_called_once_with("data1")
    assert "NonExistentAgent" not in results # Should not have a result for non-existent agent

@pytest.mark.asyncio
async def test_orchestrator_execute_workflow_agent_raises_exception():
    orchestrator = AgentOrchestrator()
    mock_agent_error = MockAgent("AgentError")
    mock_agent_error.mock_process.side_effect = Exception("Test exception")
    orchestrator.register_agent(mock_agent_error)
    
    mock_agent_success = MockAgent("AgentSuccess")
    orchestrator.register_agent(mock_agent_success)

    workflow = [
        {"agent_id": "AgentError", "input_data": "bad_data"},
        {"agent_id": "AgentSuccess", "input_data": "good_data"},
    ]

    # Expect an error log, but other agents should still run if not explicitly stopped
    results = await orchestrator.execute_workflow(workflow)

    mock_agent_error.mock_process.assert_called_once_with("bad_data")
    mock_agent_success.mock_process.assert_called_once_with("good_data")
    assert "AgentError" not in results # No result for agent that raised exception
    assert results["AgentSuccess"] == {"result": "AgentSuccess processed"}

