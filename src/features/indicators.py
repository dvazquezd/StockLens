"""Technical indicator calculation and data enrichment with OOP architecture."""

from __future__ import annotations
import warnings
from typing import Optional

import pandas as pd
import pandas_ta as ta

# Suppress pandas_ta warnings
warnings.filterwarnings(
    "ignore",
    message=r"pkg_resources is deprecated as an API.*",
    category=UserWarning,
    module=r"pandas_ta(\.__init__)?"
)


class TechnicalIndicatorCalculator:
    """Calculates technical indicators for market data."""

    @staticmethod
    def standardize_ohlcv_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize OHLCV DataFrame column names and format.

        Args:
            df: DataFrame with OHLCV data

        Returns:
            Standardized DataFrame

        Raises:
            ValueError: If essential columns are missing after standardization
        """
        df = df.copy()

        # Flatten MultiIndex columns if present
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0] for col in df.columns]

        # Handle time column
        if "time" not in df.columns:
            if isinstance(df.index, pd.DatetimeIndex) or df.index.name in ("Date", "Datetime"):
                df = df.reset_index()

            for candidate in ("time", "Date", "Datetime", "date"):
                if candidate in df.columns and candidate != "time":
                    df = df.rename(columns={candidate: "time"})
                    break

        # Standardize column names
        column_mapping = {
            "Open": "open", "High": "high", "Low": "low",
            "Close": "close", "Adj Close": "close", "Volume": "volume"
        }
        df = df.rename(columns=column_mapping)

        # Validate essential columns
        required_columns = ["time", "open", "high", "low", "close", "volume"]
        available_columns = [col for col in required_columns if col in df.columns]

        if "time" not in available_columns or "close" not in available_columns:
            raise ValueError(f"Missing essential columns after standardization: {df.columns.tolist()}")

        return df[available_columns].sort_values("time").reset_index(drop=True)

    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate technical indicators for OHLCV data.

        The following indicators are calculated:
            - rsi_14: 14-period Relative Strength Index
            - macd: MACD line (12, 26, 9)
            - macd_signal: MACD signal line (12, 26, 9)
            - atr_14: 14-period Average True Range
            - adx: 14-period Average Directional Index
            - obv: On-Balance Volume

        Args:
            df: Standardized OHLCV DataFrame

        Returns:
            DataFrame with calculated indicators (NaN rows removed)
        """
        df = self.standardize_ohlcv_columns(df)

        # Start with time and close columns
        result = df[["time", "close"]].copy()

        # Calculate indicators
        result["rsi_14"] = ta.rsi(df["close"], length=14)

        # MACD calculation
        macd_data = ta.macd(df["close"], fast=12, slow=26, signal=9)
        if macd_data is not None:
            result["macd"] = macd_data["MACD_12_26_9"]
            result["macd_signal"] = macd_data["MACDs_12_26_9"]

        # Additional indicators
        result["atr_14"] = ta.atr(df["high"], df["low"], df["close"], length=14)
        result["adx"] = ta.adx(df["high"], df["low"], df["close"], length=14)["ADX_14"]
        result["obv"] = ta.obv(df["close"], df["volume"])

        # Remove rows with NaN values
        return result.dropna().reset_index(drop=True)


# Backward compatibility function
def enrich_with_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enriches a standardized OHLCV dataset with common technical analysis indicators.
    (Backward compatibility wrapper)

    Args:
        df: A DataFrame containing OHLCV data

    Returns:
        DataFrame with time, close, and calculated indicator columns
    """
    calculator = TechnicalIndicatorCalculator()
    return calculator.calculate_indicators(df)
