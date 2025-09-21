"""Market data ingestion and normalization."""

from pathlib import Path
from typing import Optional

import pandas as pd
import yfinance as yf

from .binance_client import BinanceDataClient


class MarketDataNormalizer:
    """Normalizes market data from different sources to a standard format."""
    
    @staticmethod
    def normalize_yahoo_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize Yahoo Finance data to standard OHLCV format.
        
        Args:
            df: Raw Yahoo Finance DataFrame
            
        Returns:
            Normalized DataFrame with standard column names
        """
        # Flatten MultiIndex columns if present
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0] for col in df.columns]
        
        # Move Date/Datetime index to column if needed
        if not any(col in df.columns for col in ["Date", "Datetime"]):
            if isinstance(df.index, pd.DatetimeIndex) or df.index.name in ("Date", "Datetime"):
                df = df.reset_index()
        
        # Prefer Close over Adj Close
        if "Close" in df.columns and "Adj Close" in df.columns:
            df = df.drop(columns=["Adj Close"])
        
        # Standardize column names
        column_mapping = {
            "Date": "time", "Datetime": "time",
            "Open": "open", "High": "high", "Low": "low",
            "Close": "close", "Adj Close": "close",
            "Volume": "volume"
        }
        df = df.rename(columns=column_mapping)
        
        # Select and validate required columns
        required_columns = ["time", "open", "high", "low", "close", "volume"]
        available_columns = [col for col in required_columns if col in df.columns]
        
        if "time" not in available_columns or "close" not in available_columns:
            raise ValueError(f"Missing essential columns after normalization: {df.columns.tolist()}")
        
        # Remove duplicate columns and sort
        df = df.loc[:, ~df.columns.duplicated()]
        return df[available_columns].sort_values("time").reset_index(drop=True)
    
    @staticmethod
    def normalize_binance_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate and sort Binance data.
        
        Args:
            df: Binance OHLCV DataFrame
            
        Returns:
            Validated and sorted DataFrame
        """
        required_columns = ["time", "open", "high", "low", "close", "volume"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        return df.sort_values("time").reset_index(drop=True)


class MarketDataDownloader:
    """Downloads and processes market data from various sources."""
    
    def __init__(self):
        self._binance_client = BinanceDataClient()
        self._normalizer = MarketDataNormalizer()
    
    def download_data(
        self,
        symbol: str,
        source: str,
        interval: str,
        limit: Optional[int] = None,
        period: Optional[str] = None,
        save_to_disk: bool = True,
        output_directory: Optional[Path] = None,
    ) -> pd.DataFrame:
        """
        Download market data from specified source.
        
        Args:
            symbol: Asset symbol or ticker
            source: Data source ('binance' or 'yahoo')
            interval: Time interval
            limit: Number of data points (Binance only)
            period: Time period (Yahoo only)
            save_to_disk: Whether to save data to disk
            output_directory: Directory to save data
            
        Returns:
            Normalized OHLCV DataFrame
        """
        if source == "binance":
            if limit is None:
                raise ValueError("Parameter 'limit' is required for Binance data")
            
            df = self._binance_client.get_ohlcv_data(symbol=symbol, interval=interval, limit=limit)
            df = self._normalizer.normalize_binance_data(df)
            
        elif source == "yahoo":
            if period is None:
                raise ValueError("Parameter 'period' is required for Yahoo data")
            
            df = yf.download(symbol, interval=interval, period=period, auto_adjust=False)
            if df.empty:
                raise ValueError(f"No data returned from Yahoo for symbol: {symbol}")
            
            df = self._normalizer.normalize_yahoo_data(df)
            
        else:
            raise ValueError(f"Unsupported data source: {source}")
        
        if save_to_disk:
            output_dir = output_directory or Path("data/raw")
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / f"{symbol}_{interval}.parquet"
            df.to_parquet(output_file, index=False)
            print(f"Saved {symbol} data to {output_file} ({len(df)} rows)")
        
        return df