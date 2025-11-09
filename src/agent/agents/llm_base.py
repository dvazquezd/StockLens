"""Base class for LLM-based trading agents."""

import json
import re
from abc import abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.agent.agents.base import TradingAgent
from config.config import PROMPT_PATH


class LLMAgent(TradingAgent):
    """Base class for all LLM-based trading agents."""

    def __init__(self, model: str, api_key: Optional[str] = None):
        """
        Initialize LLM agent.

        Args:
            model: Model identifier (e.g., 'gpt-4o-mini', 'claude-opus-4')
            api_key: API key for the LLM provider
        """
        self.model = model
        self.api_key = api_key
        self.provider_name = self.__class__.__name__.replace("Agent", "").lower()

    @abstractmethod
    def _call_llm(self, prompt: str) -> str:
        """
        Call the LLM API with the given prompt.

        Args:
            prompt: The prompt to send to the LLM

        Returns:
            Raw text response from the LLM

        Raises:
            Exception: If API call fails
        """
        pass

    @abstractmethod
    def _validate_api_key(self) -> bool:
        """
        Validate that API key is present and valid.

        Returns:
            True if API key is valid, False otherwise
        """
        pass

    def analyze_signals(self, processed_dir: Path) -> Dict[str, Any]:
        """
        Analyze trading signals using LLM.

        Args:
            processed_dir: Path to directory containing signal files

        Returns:
            Dictionary containing analysis results
        """
        print(f"\nExecuting {self.provider_name.title()} Agent with model {self.model}")

        # Load signal data
        signal_data = self._load_signal_data(processed_dir, num_rows=5)

        # Check if API key is available
        if not self._validate_api_key():
            return self._create_offline_draft(processed_dir, signal_data)

        # Generate prompt and call LLM
        prompt = self._build_prompt(signal_data)

        try:
            response_text = self._call_llm(prompt)
            return self._process_llm_response(response_text, processed_dir, signal_data)

        except Exception as e:
            print(f"Error calling {self.provider_name} API: {e}")
            return self._create_error_fallback(processed_dir, signal_data, str(e))

    def _build_prompt(self, signal_data: List[Dict[str, Any]]) -> str:
        """
        Build the complete prompt for the LLM.

        Args:
            signal_data: List of signal data dictionaries

        Returns:
            Complete prompt string
        """
        base_prompt = self._load_base_prompt()
        assets_json = json.dumps(signal_data, ensure_ascii=False, default=str)

        json_instruction = (
            "\n\nRespond ONLY with a valid JSON array containing objects with "
            "'symbol', 'recommendation', and 'rationale' fields. "
            "Do not include any other text or explanations outside the JSON."
        )

        return f"{base_prompt}{json_instruction}\n\nHere is the data:\n{assets_json}"

    def _load_base_prompt(self) -> str:
        """Load the base prompt from configuration file."""
        return PROMPT_PATH.read_text(encoding="utf-8")

    def _extract_json_from_response(self, text: str) -> str:
        """
        Extract JSON content from LLM response.

        Handles responses with markdown code blocks or extra text.

        Args:
            text: Raw LLM response text

        Returns:
            Extracted JSON string
        """
        # Remove markdown code blocks
        cleaned = text.strip()
        cleaned = re.sub(r'^```json\s*', '', cleaned, flags=re.MULTILINE)
        cleaned = re.sub(r'^```\s*', '', cleaned, flags=re.MULTILINE)
        cleaned = re.sub(r'\s*```$', '', cleaned, flags=re.MULTILINE)
        cleaned = cleaned.strip()

        # Try to find JSON array or object using regex
        json_array_pattern = r'\[\s*\{.*?\}\s*\]'
        json_object_pattern = r'\{.*?\}'

        # Try to match JSON array first (most common for this use case)
        match = re.search(json_array_pattern, cleaned, re.DOTALL)
        if match:
            return match.group(0)

        # Try to match JSON object
        match = re.search(json_object_pattern, cleaned, re.DOTALL)
        if match:
            return match.group(0)

        # If no pattern matched, return cleaned text
        return cleaned

    def _process_llm_response(
        self,
        response_text: str,
        processed_dir: Path,
        signal_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Process and validate LLM response.

        Args:
            response_text: Raw LLM response
            processed_dir: Directory to save results
            signal_data: Original signal data

        Returns:
            Validated response dictionary
        """
        try:
            # Extract JSON from response
            cleaned_response = self._extract_json_from_response(response_text)
            response_json = json.loads(cleaned_response)

            # Validate response structure
            if not isinstance(response_json, list):
                raise ValueError(f"Expected JSON array, got {type(response_json).__name__}")

            # Validate each recommendation
            required_fields = {"symbol", "recommendation", "rationale"}
            for idx, item in enumerate(response_json):
                if not isinstance(item, dict):
                    raise ValueError(f"Item {idx} is not a dict: {type(item).__name__}")

                missing_fields = required_fields - set(item.keys())
                if missing_fields:
                    raise ValueError(f"Item {idx} missing fields: {missing_fields}")

                # Validate recommendation value
                valid_recommendations = {"buy", "sell", "hold"}
                rec = item.get("recommendation", "").lower()
                if rec not in valid_recommendations:
                    print(f"Warning: Invalid recommendation '{rec}' in {item.get('symbol')}. Defaulting to 'hold'.")
                    item["recommendation"] = "hold"

            # Save successfully parsed and validated JSON
            output_file = processed_dir / "agent_summary_llm.json"
            output_file.write_text(
                json.dumps(response_json, ensure_ascii=False, indent=2, default=str),
                encoding="utf-8"
            )
            print(f"{self.provider_name.title()} Agent -> {output_file}")
            print(f"Successfully processed {len(response_json)} recommendations")

            return response_json

        except (json.JSONDecodeError, ValueError) as e:
            print(f"Error parsing JSON from {self.provider_name}: {e}")
            print(f"Raw response (first 500 chars): {response_text[:500]}...")
            return self._create_error_fallback(processed_dir, signal_data, str(e), response_text)

    def _create_offline_draft(
        self,
        processed_dir: Path,
        signal_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Create offline draft when API key is not available.

        Args:
            processed_dir: Directory to save draft
            signal_data: Signal data to include in draft

        Returns:
            Draft dictionary
        """
        draft = {
            "provider": self.provider_name,
            "model": self.model,
            "note": f"API key not found; offline summary generated.",
            "assets": signal_data,
        }

        output_file = processed_dir / "agent_summary_llm.json"
        output_file.write_text(
            json.dumps(draft, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8"
        )
        print(f"{self.provider_name.title()} Agent (draft) -> {output_file}")
        return draft

    def _create_error_fallback(
        self,
        processed_dir: Path,
        signal_data: List[Dict[str, Any]],
        error_message: str,
        raw_response: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create error fallback document for debugging.

        Args:
            processed_dir: Directory to save error document
            signal_data: Original signal data
            error_message: Error message
            raw_response: Raw LLM response if available

        Returns:
            Error fallback dictionary
        """
        fallback = {
            "provider": self.provider_name,
            "model": self.model,
            "timestamp": datetime.now().isoformat(),
            "error": error_message,
            "assets_analyzed": len(signal_data)
        }

        if raw_response:
            fallback["raw_response"] = raw_response

        output_file = processed_dir / "agent_summary_llm_error.json"
        output_file.write_text(
            json.dumps(fallback, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8"
        )
        print(f"{self.provider_name.title()} Agent (error fallback) -> {output_file}")
        print("Check error file for debugging")

        return fallback
