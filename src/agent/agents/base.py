"""Base class for all trading agents."""

import json
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
        Load signal data from parquet files with portfolio information.

        Args:
            processed_dir: Path to directory containing *_signals.parquet files
            num_rows: Number of recent rows to load per symbol

        Returns:
            List of dictionaries containing symbol, signal data, and portfolio info
        """
        # Load assets config to get portfolio information
        assets_config = self._load_assets_config()

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

            item = {
                "symbol": symbol,
                "last": last_rows.to_dict(orient="records")
            }

            # Add portfolio information if asset is in portfolio
            if symbol in assets_config:
                asset_info = assets_config[symbol]
                if asset_info.get("in_portfolio", False):
                    # Get current price from last row
                    current_price = float(last_rows.iloc[-1].get("close", 0))

                    item["portfolio"] = {
                        "in_portfolio": True,
                        "purchase_date": asset_info.get("purchase_date"),
                        "purchase_price": asset_info.get("purchase_price"),
                        "shares": asset_info.get("shares"),
                        "current_price": current_price
                    }

                    # Calculate P&L if we have purchase info
                    if asset_info.get("purchase_price") and asset_info.get("shares"):
                        cost_basis = asset_info["purchase_price"] * asset_info["shares"]
                        current_value = current_price * asset_info["shares"]
                        pnl_amount = current_value - cost_basis
                        pnl_percent = (pnl_amount / cost_basis * 100) if cost_basis > 0 else 0

                        item["portfolio"]["cost_basis"] = cost_basis
                        item["portfolio"]["current_value"] = current_value
                        item["portfolio"]["pnl_amount"] = pnl_amount
                        item["portfolio"]["pnl_percent"] = pnl_percent
                else:
                    item["portfolio"] = {"in_portfolio": False}

            items.append(item)

        return items

    def _load_assets_config(self) -> Dict[str, Dict]:
        """
        Load assets configuration from JSON file.

        Returns:
            Dictionary mapping symbol to asset config
        """
        config_path = Path("config/assets_config.json")

        if not config_path.exists():
            return {}

        try:
            with open(config_path, 'r') as f:
                assets = json.load(f)

            # Convert list to dict keyed by symbol
            return {asset['symbol']: asset for asset in assets}
        except (json.JSONDecodeError, FileNotFoundError):
            return {}

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
