from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

RAW_PATH = Path(os.getenv("RAW_PATH", "./data/raw"))
PROCESSED_PATH = Path(os.getenv("PROCESSED_PATH", "./data/processed"))
ASSETS_CONFIG = Path(os.getenv("ASSETS_CONFIG", "./config/assets_config.json"))

DEFAULT_INTERVAL = os.getenv("DEFAULT_INTERVAL", "1d")
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "1000"))
DEFAULT_PERIOD = os.getenv("DEFAULT_PERIOD", "1y")

RAW_PATH.mkdir(parents=True, exist_ok=True)
PROCESSED_PATH.mkdir(parents=True, exist_ok=True)


