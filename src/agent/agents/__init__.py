"""Trading agent implementations with OOP architecture."""

from src.agent.agents.base import TradingAgent
from src.agent.agents.factory import AgentFactory
from src.agent.agents.local_agent import LocalAgent
from src.agent.agents.anthropic_agent import AnthropicAgent
from src.agent.agents.openai_agent import OpenAIAgent

__all__ = [
    "TradingAgent",
    "AgentFactory",
    "LocalAgent",
    "AnthropicAgent",
    "OpenAIAgent",
]
