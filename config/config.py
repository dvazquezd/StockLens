"""Configuration module for StockLens trading analysis system."""

import os
from pathlib import Path
from typing import Literal

from dotenv import load_dotenv

load_dotenv()

# Data paths
RAW_DATA_PATH = Path(os.getenv("RAW_PATH", "./data/raw"))
PROCESSED_DATA_PATH = Path(os.getenv("PROCESSED_PATH", "./data/processed"))
ASSETS_CONFIG_PATH = Path(os.getenv("ASSETS_CONFIG", "./config/assets_config.json"))
PROMPT_FILE_PATH = Path(os.getenv("PROMPT_PATH", "./config/agent_prompt.txt")).resolve()

# Data ingestion defaults
DEFAULT_INTERVAL = os.getenv("DEFAULT_INTERVAL", "1d")
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "1000"))
DEFAULT_PERIOD = os.getenv("DEFAULT_PERIOD", "1y")

# Agent configuration
AgentMode = Literal["local", "llm"]
LLMProvider = Literal["openai", "anthropic"]

AGENT_MODE: AgentMode = os.getenv("AGENT_MODE", "llm").lower()
LLM_MODEL = os.getenv("LLM_MODEL", "claude-opus-4-1-20250805")
LLM_PROVIDER: LLMProvider = os.getenv("LLM_PROVIDER", "anthropic")

# Create directories
RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_PATH.mkdir(parents=True, exist_ok=True)
