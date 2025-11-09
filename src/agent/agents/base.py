"""Base class for all trading agents."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


class TradingAgent(ABC):
    """Abstract base class for trading analysis agents."""

    @abstractmethod
    def analyze_signals(self, processed_dir: Path) -> Any:
        """
        Analyze trading signals and generate recommendations.

        Args:
            processed_dir: Path to directory containing processed signal files

        Returns:
            Analysis results (format depends on implementation)
        """
        pass

    def _load_signal_data(self, processed_dir: Path, num_rows: int = 5) -> List[Dict[str, Any]]:
        """
        Load signal data from parquet files.

        Args:
            processed_dir: Path to directory containing *_signals.parquet files
            num_rows: Number of recent rows to load per symbol

        Returns:
            List of dictionaries containing symbol and signal data
        """
        items = []

        for file_path in sorted(processed_dir.glob("*_signals.parquet")):
            symbol = file_path.name.split("_")[0]
            df = pd.read_parquet(file_path)

            # Get last N rows
            last_rows = df.tail(num_rows).copy()

            # Normalize time column to ISO string if present
            if "time" in last_rows.columns:
                last_rows["time"] = pd.to_datetime(
                    last_rows["time"],
                    utc=False,
                    errors="coerce"
                ).dt.strftime("%Y-%m-%dT%H:%M:%S")

            items.append({
                "symbol": symbol,
                "last": last_rows.to_dict(orient="records")
            })

        return items

    def _get_latest_row(self, processed_dir: Path, symbol: str) -> pd.Series:
        """
        Get the latest signal row for a specific symbol.

        Args:
            processed_dir: Path to directory containing signal files
            symbol: Asset symbol

        Returns:
            Latest row as pandas Series
        """
        for file_path in processed_dir.glob(f"{symbol}_*_signals.parquet"):
            df = pd.read_parquet(file_path)
            return df.iloc[-1]

        raise FileNotFoundError(f"No signal file found for symbol: {symbol}")
