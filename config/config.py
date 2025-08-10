import os
from dotenv import load_dotenv
from pathlib import Path

# Cargar variables desde .env
load_dotenv()

RAW_PATH = Path(os.getenv("RAW_PATH", "./data/raw"))
DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
PROCESSED_PATH = Path(os.getenv("PROCESSED_PATH", "./data/processed"))
CONFIG_FILE = Path(os.getenv("CONFIG_FILE", "assets_config.json"))

DEFAULT_INTERVAL = os.getenv("DEFAULT_INTERVAL", "1d")
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "1000"))
DEFAULT_PERIOD = os.getenv("DEFAULT_PERIOD", "1y")

# Asegurar que las carpetas existen
RAW_PATH.mkdir(parents=True, exist_ok=True)
PROCESSED_PATH.mkdir(parents=True, exist_ok=True)
DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
