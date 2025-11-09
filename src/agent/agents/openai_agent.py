"""OpenAI GPT trading agent implementation."""

import os
from typing import Optional

from openai import OpenAI

from src.agent.agents.llm_base import LLMAgent


class OpenAIAgent(LLMAgent):
    """Trading agent using OpenAI GPT models."""

    def __init__(self, model: str = "gpt-4o-mini", api_key: Optional[str] = None):
        """
        Initialize OpenAI agent.

        Args:
            model: OpenAI model identifier (e.g., 'gpt-4o', 'gpt-4o-mini', 'gpt-4-turbo')
            api_key: OpenAI API key (if None, reads from OAIKEY env var)
        """
        if api_key is None:
            api_key = os.getenv("OAIKEY")

        super().__init__(model=model, api_key=api_key)
        self._client = None

    def _validate_api_key(self) -> bool:
        """
        Validate that OpenAI API key is present.

        Returns:
            True if API key is available, False otherwise
        """
        return self.api_key is not None and len(self.api_key) > 0

    def _call_llm(self, prompt: str) -> str:
        """
        Call OpenAI GPT API.

        Args:
            prompt: The prompt to send to GPT

        Returns:
            Raw text response from GPT

        Raises:
            Exception: If API call fails
        """
        if self._client is None:
            self._client = OpenAI(api_key=self.api_key)

        response = self._client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "You are a financial market analysis assistant."},
                {"role": "user", "content": prompt}
            ]
        )

        # Extract text from OpenAI response
        return response.choices[0].message.content
