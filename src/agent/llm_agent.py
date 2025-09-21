"""LLM-based trading analysis agent."""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Union

import pandas as pd
from anthropic import Anthropic
from openai import OpenAI

from config.config import PROMPT_FILE_PATH, LLMProvider


class LLMTradingAgent:
    """LLM-powered trading analysis agent supporting multiple providers."""
    
    def __init__(self, model: str, provider: LLMProvider):
        self.model = model
        self.provider = provider
        self._client = self._initialize_client()
    
    def _initialize_client(self) -> Union[OpenAI, Anthropic]:
        """Initialize the appropriate LLM client based on provider."""
        if self.provider == "anthropic":
            api_key = os.getenv("ANTHROPIC_STOCK_LENS")
            if not api_key:
                raise ValueError("ANTHROPIC_STOCK_LENS environment variable not found")
            return Anthropic(api_key=api_key)
        
        elif self.provider == "openai":
            api_key = os.getenv("OAIKEY")
            if not api_key:
                raise ValueError("OAIKEY environment variable not found")
            return OpenAI(api_key=api_key)
        
        else:
            raise ValueError(f"Unsupported LLM provider: {self.provider}")
    
    def _load_prompt_template(self) -> str:
        """Load the base prompt template from configuration file."""
        try:
            return PROMPT_FILE_PATH.read_text(encoding="utf-8")
        except FileNotFoundError:
            raise FileNotFoundError(f"Prompt file not found: {PROMPT_FILE_PATH}")
    
    def _prepare_signal_data(self, processed_directory: Path) -> List[Dict]:
        """
        Load and prepare the latest signal data from processed files.
        
        Args:
            processed_directory: Directory containing processed signal files
            
        Returns:
            List of dictionaries containing symbol and latest signal data
        """
        signal_files = list(processed_directory.glob("*_signals.parquet"))
        if not signal_files:
            raise FileNotFoundError(f"No signal files found in {processed_directory}")
        
        prepared_data = []
        
        for file_path in sorted(signal_files):
            symbol = file_path.name.split("_")[0]
            df = pd.read_parquet(file_path)
            
            # Get last 5 rows and normalize time column
            latest_data = df.tail(5).copy()
            if "time" in latest_data.columns:
                latest_data["time"] = pd.to_datetime(
                    latest_data["time"], utc=False, errors="coerce"
                ).dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            
            prepared_data.append({
                "symbol": symbol,
                "latest_signals": latest_data.to_dict(orient="records")
            })
        
        return prepared_data
    
    def _generate_completion(self, prompt: str) -> str:
        """
        Generate completion using the configured LLM provider.
        
        Args:
            prompt: The complete prompt to send to the LLM
            
        Returns:
            Generated text response
        """
        if self.provider == "anthropic":
            response = self._client.messages.create(
                model=self.model,
                max_tokens=4000,
                messages=[{"role": "user", "content": prompt}]
            )
            return response.content[0].text
        
        elif self.provider == "openai":
            response = self._client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a financial market analysis assistant."},
                    {"role": "user", "content": prompt}
                ]
            )
            return response.choices[0].message.content
        
        else:
            raise ValueError(f"Unsupported provider: {self.provider}")
    
    def _parse_llm_response(self, response_text: str) -> Union[List[Dict], str]:
        """
        Parse LLM response as JSON, with fallback handling.
        
        Args:
            response_text: Raw response from LLM
            
        Returns:
            Parsed JSON data or original text if parsing fails
        """
        try:
            # Clean potential markdown formatting
            cleaned_response = response_text.strip()
            if cleaned_response.startswith('```json'):
                cleaned_response = cleaned_response.replace('```json', '').replace('```', '').strip()
            elif cleaned_response.startswith('```'):
                cleaned_response = cleaned_response.replace('```', '').strip()
            
            return json.loads(cleaned_response)
        
        except json.JSONDecodeError as e:
            print(f"JSON parsing failed for {self.provider}: {e}")
            print(f"Original response: {response_text}")
            return response_text
    
    def analyze_signals(self, processed_directory: Path) -> Union[List[Dict], Dict]:
        """
        Analyze trading signals using LLM and generate recommendations.
        
        Args:
            processed_directory: Directory containing processed signal data
            
        Returns:
            Analysis results as JSON data or error information
        """
        print(f"\nExecuting LLM agent with model {self.model} and provider {self.provider}")
        
        try:
            # Prepare data
            signal_data = self._prepare_signal_data(processed_directory)
            
            # Load prompt and prepare payload
            base_prompt = self._load_prompt_template()
            data_json = json.dumps(signal_data, ensure_ascii=False, default=str)
            complete_prompt = f"{base_prompt}\n\nHere is the data:\n{data_json}"
            
            # Generate analysis
            response_text = self._generate_completion(complete_prompt)
            parsed_response = self._parse_llm_response(response_text)
            
            # Save results
            if isinstance(parsed_response, list):
                output_file = processed_directory / "agent_summary_llm.json"
                with output_file.open('w', encoding='utf-8') as f:
                    json.dump(parsed_response, f, ensure_ascii=False, indent=2, default=str)
                print(f"LLM agent ({self.provider}) -> {output_file}")
                return parsed_response
            
            else:
                # Handle parsing error
                error_data = {
                    "provider": self.provider,
                    "model": self.model,
                    "timestamp": datetime.now().isoformat(),
                    "error": "Failed to parse JSON response",
                    "raw_response": response_text,
                    "assets_analyzed": len(signal_data)
                }
                
                error_file = processed_directory / "agent_summary_llm_error.json"
                with error_file.open('w', encoding='utf-8') as f:
                    json.dump(error_data, f, ensure_ascii=False, indent=2, default=str)
                print(f"LLM agent (error fallback) -> {error_file}")
                return error_data
                
        except Exception as e:
            # Handle missing API key or other errors
            print(f"LLM agent execution failed: {e}")
            
            # Generate offline draft
            try:
                signal_data = self._prepare_signal_data(processed_directory)
                draft_data = {
                    "provider": self.provider,
                    "model": self.model,
                    "note": f"LLM agent failed: {str(e)}",
                    "assets": signal_data,
                }
                
                draft_file = processed_directory / "agent_summary_llm.json"
                with draft_file.open('w', encoding='utf-8') as f:
                    json.dump(draft_data, f, ensure_ascii=False, indent=2, default=str)
                print(f"LLM agent (draft) -> {draft_file}")
                return draft_data
                
            except Exception as draft_error:
                print(f"Failed to create draft: {draft_error}")
                raise