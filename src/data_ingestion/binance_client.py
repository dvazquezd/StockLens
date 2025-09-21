"""Binance API client for market data retrieval."""

import os
from typing import Optional

import pandas as pd
from binance.client import Client


class BinanceDataClient:
    """Client for retrieving market data from Binance."""
    
    def __init__(self) -> None:
        """Initialize Binance client with optional API credentials."""
        api_key = os.getenv("BINANCE_API_KEY")
        api_secret = os.getenv("BINANCE_API_SECRET")
        self._client = Client(api_key=api_key, api_secret=api_secret)
    
    def get_ohlcv_data(self, symbol: str, interval: str, limit: int = 1000) -> pd.DataFrame:
        """
        Retrieve OHLCV data from Binance.
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            interval: Time interval (e.g., '1d', '1h')
            limit: Number of data points to retrieve
            
        Returns:
            DataFrame with columns: time, open, high, low, close, volume
        """
        klines = self._client.get_klines(symbol=symbol, interval=interval, limit=limit)
        
        columns = [
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base", "taker_buy_quote", "ignore",
        ]
        
        df = pd.DataFrame(klines, columns=columns)
        
        # Convert numeric columns
        numeric_columns = ["open", "high", "low", "close", "volume"]
        df[numeric_columns] = df[numeric_columns].astype(float)
        
        # Convert timestamp
        df["time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        
        # Return standardized columns
        return df[["time", "open", "high", "low", "close", "volume"]].sort_values("time").reset_index(drop=True)
