"""Factory for creating trading agent instances."""

from typing import Optional

from src.agent.agents.base import TradingAgent
from src.agent.agents.local_agent import LocalAgent
from src.agent.agents.anthropic_agent import AnthropicAgent
from src.agent.agents.openai_agent import OpenAIAgent


class AgentFactory:
    """Factory for creating trading agent instances."""

    @staticmethod
    def create_agent(
        provider: str,
        model: Optional[str] = None,
        api_key: Optional[str] = None
    ) -> TradingAgent:
        """
        Create a trading agent instance based on provider.

        Args:
            provider: Agent provider ('local', 'anthropic', 'openai')
            model: Model identifier (optional, uses defaults if not provided)
            api_key: API key for LLM providers (optional, reads from env if not provided)

        Returns:
            TradingAgent instance

        Raises:
            ValueError: If provider is not supported

        Examples:
            >>> agent = AgentFactory.create_agent('local')
            >>> agent = AgentFactory.create_agent('anthropic', model='claude-opus-4')
            >>> agent = AgentFactory.create_agent('openai', model='gpt-4o')
        """
        provider = provider.lower()

        if provider == "local":
            return LocalAgent()

        elif provider == "anthropic":
            model = model or "claude-opus-4-1-20250805"
            return AnthropicAgent(model=model, api_key=api_key)

        elif provider == "openai":
            model = model or "gpt-4o-mini"
            return OpenAIAgent(model=model, api_key=api_key)

        else:
            raise ValueError(
                f"Unsupported agent provider: {provider}. "
                f"Supported providers: 'local', 'anthropic', 'openai'"
            )

    @staticmethod
    def get_supported_providers() -> list[str]:
        """
        Get list of supported agent providers.

        Returns:
            List of supported provider names
        """
        return ["local", "anthropic", "openai"]
