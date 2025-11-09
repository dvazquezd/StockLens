"""Anthropic Claude trading agent implementation."""

import os
from typing import Optional

from anthropic import Anthropic

from src.agent.agents.llm_base import LLMAgent


class AnthropicAgent(LLMAgent):
    """Trading agent using Anthropic Claude models."""

    def __init__(self, model: str = "claude-opus-4-1-20250805", api_key: Optional[str] = None):
        """
        Initialize Anthropic agent.

        Args:
            model: Anthropic model identifier (e.g., 'claude-opus-4', 'claude-sonnet-4')
            api_key: Anthropic API key (if None, reads from ANTHROPIC_STOCK_LENS env var)
        """
        if api_key is None:
            api_key = os.getenv("ANTHROPIC_STOCK_LENS")

        super().__init__(model=model, api_key=api_key)
        self._client = None

    def _validate_api_key(self) -> bool:
        """
        Validate that Anthropic API key is present.

        Returns:
            True if API key is available, False otherwise
        """
        return self.api_key is not None and len(self.api_key) > 0

    def _call_llm(self, prompt: str) -> str:
        """
        Call Anthropic Claude API.

        Args:
            prompt: The prompt to send to Claude

        Returns:
            Raw text response from Claude

        Raises:
            Exception: If API call fails
        """
        if self._client is None:
            self._client = Anthropic(api_key=self.api_key)

        response = self._client.messages.create(
            model=self.model,
            max_tokens=4000,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )

        # Extract text from Anthropic response
        return response.content[0].text
