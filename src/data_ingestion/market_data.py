"""Market data ingestion and normalization with OOP architecture."""

from __future__ import annotations
from pathlib import Path
from typing import Optional

import pandas as pd
import yfinance as yf

from src.data_ingestion.binance_client import download_ohlcv
from src.database.data_cache import DataCache


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

        Raises:
            ValueError: If essential columns are missing after normalization
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

        Raises:
            ValueError: If required columns are missing
        """
        required_columns = ["time", "open", "high", "low", "close", "volume"]
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        return df.sort_values("time").reset_index(drop=True)


class MarketDataDownloader:
    """Downloads and processes market data from various sources with intelligent caching."""

    def __init__(self, db_path: str = "data/stocklens.db"):
        """
        Initialize the market data downloader.

        Args:
            db_path: Path to SQLite database for caching
        """
        self.db_path = db_path
        self._normalizer = MarketDataNormalizer()

    def download_data(
        self,
        symbol: str,
        source: str,
        interval: str,
        limit: Optional[int] = None,
        period: Optional[str] = None,
        use_cache: bool = True,
        save_to_disk: bool = True,
        output_directory: Optional[Path] = None,
    ) -> pd.DataFrame:
        """
        Download market data from specified source with intelligent caching.

        Args:
            symbol: Asset symbol or ticker
            source: Data source ('binance' or 'yahoo')
            interval: Time interval
            limit: Number of data points (Binance only)
            period: Time period (Yahoo only)
            use_cache: Whether to use intelligent caching
            save_to_disk: Whether to save data to disk (parquet files)
            output_directory: Directory to save data

        Returns:
            Normalized OHLCV DataFrame

        Raises:
            ValueError: If required parameters are missing or source is unsupported
        """
        if use_cache:
            return self._download_with_cache(
                symbol=symbol,
                source=source,
                interval=interval,
                limit=limit,
                period=period,
                save_to_disk=save_to_disk,
                output_directory=output_directory,
            )
        else:
            return self._download_direct(
                symbol=symbol,
                source=source,
                interval=interval,
                limit=limit,
                period=period,
                save_to_disk=save_to_disk,
                output_directory=output_directory,
            )

    def _download_with_cache(
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
        Download data with intelligent caching (downloads only new data).

        This method:
        1. Checks if data exists in cache
        2. Determines if cache is fresh or needs updating
        3. Downloads only missing/new data
        4. Merges with cached data
        5. Saves to database
        """
        with DataCache(self.db_path) as cache:
            if source == "binance":
                if limit is None:
                    raise ValueError("Parameter 'limit' is required for Binance data")

                use_cached, download_limit, latest_ts = cache.get_download_params(
                    symbol=symbol,
                    source=source,
                    interval=interval,
                    requested_limit=limit
                )

                if download_limit == 0:
                    # Cache is fresh and sufficient
                    print(f"✓ {symbol}: Using cached data ({limit} rows, fresh)")
                    cached_df, _ = cache.get_cached_data(symbol, source, interval, limit)

                    if save_to_disk:
                        self._save_to_disk(cached_df, symbol, interval, output_directory)

                    return cached_df

                # Download new data
                cached_df, _ = cache.get_cached_data(symbol, source, interval)
                cache_size = len(cached_df) if cached_df is not None else 0
                print(f"📥 {symbol}: Downloading {download_limit} new rows (cache has {cache_size} rows)")

                new_df = download_ohlcv(symbol=symbol, interval=interval, limit=download_limit)
                new_df = self._normalizer.normalize_binance_data(new_df)

                # Merge with cache
                merged_df = cache.merge_with_cache(new_df, symbol, source, interval, limit=limit)
                print(f"✓ {symbol}: {len(new_df)} new rows downloaded, total {len(merged_df)} rows in cache")

                if save_to_disk:
                    self._save_to_disk(merged_df, symbol, interval, output_directory)

                return merged_df

            elif source == "yahoo":
                if period is None:
                    raise ValueError("Parameter 'period' is required for Yahoo data")

                # For Yahoo, we always need to download full period (no incremental support in yfinance)
                # But we can still cache it
                cached_df, latest_ts = cache.get_cached_data(symbol, source, interval)

                if cached_df is not None and not cache.needs_update(latest_ts, interval):
                    print(f"✓ {symbol}: Using cached data ({len(cached_df)} rows, fresh)")

                    if save_to_disk:
                        self._save_to_disk(cached_df, symbol, interval, output_directory)

                    return cached_df

                # Download from Yahoo
                print(f"📥 {symbol}: Downloading from Yahoo Finance (period: {period})")
                new_df = yf.download(symbol, interval=interval, period=period, auto_adjust=False)

                if new_df.empty:
                    raise ValueError(f"No data returned from Yahoo for symbol: {symbol}")

                new_df = self._normalizer.normalize_yahoo_data(new_df)

                # Save to cache
                cache.save_to_cache(new_df, symbol, source, interval)
                print(f"✓ {symbol}: {len(new_df)} rows downloaded and cached")

                if save_to_disk:
                    self._save_to_disk(new_df, symbol, interval, output_directory)

                return new_df

            else:
                raise ValueError(f"Unsupported data source: {source}")

    def _download_direct(
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
        Download data directly without caching.

        Args:
            symbol: Asset symbol
            source: Data source
            interval: Time interval
            limit: Number of data points (Binance)
            period: Time period (Yahoo)
            save_to_disk: Whether to save to disk
            output_directory: Directory to save data

        Returns:
            Normalized OHLCV DataFrame
        """
        if source == "binance":
            if limit is None:
                raise ValueError("Parameter 'limit' is required for Binance data")

            df = download_ohlcv(symbol=symbol, interval=interval, limit=limit)
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
            self._save_to_disk(df, symbol, interval, output_directory)

        return df

    def _save_to_disk(
        self,
        df: pd.DataFrame,
        symbol: str,
        interval: str,
        output_directory: Optional[Path] = None
    ) -> None:
        """Save DataFrame to parquet file."""
        output_dir = output_directory or Path("data/raw")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{symbol}_{interval}.parquet"
        df.to_parquet(output_file, index=False)
        print(f"Saved {symbol} data to {output_file} ({len(df)} rows)")


# Backward compatibility functions
def download_market_data_cached(
    symbol: str,
    source: str,
    interval: str,
    limit: Optional[int] = None,
    period: Optional[str] = None,
    use_cache: bool = True,
    db_path: str = "data/stocklens.db",
) -> pd.DataFrame:
    """
    Downloads market data with intelligent caching (backward compatibility wrapper).

    Args:
        symbol: Asset symbol (e.g., 'BTCUSDT', 'AAPL')
        source: Data source ('binance', 'yahoo')
        interval: Time interval ('1h', '1d', etc.)
        limit: Total number of rows desired (Binance)
        period: Historical period (Yahoo, e.g., '1y', '6mo')
        use_cache: Whether to use caching (default: True)
        db_path: Path to SQLite database

    Returns:
        DataFrame with OHLCV data (from cache + new download)
    """
    downloader = MarketDataDownloader(db_path=db_path)
    return downloader.download_data(
        symbol=symbol,
        source=source,
        interval=interval,
        limit=limit,
        period=period,
        use_cache=use_cache,
        save_to_disk=False,
    )


def download_market_data(
    symbol: str,
    source: str,
    interval: str,
    limit: Optional[int] = None,
    period: Optional[str] = None,
    to_disk: bool = True,
    raw_dir: Optional[Path] = None,
) -> pd.DataFrame:
    """
    Downloads market OHLCV data without caching (backward compatibility wrapper).

    Args:
        symbol: The asset ticker or trading pair (e.g., 'BTCUSDT', 'AAPL')
        source: Data source identifier, either 'binance' or 'yahoo'
        interval: Time interval between data points (e.g., '1h', '1d')
        limit: Number of data points to fetch (Binance only)
        period: Historical period to fetch (Yahoo only)
        to_disk: Whether to save the normalized DataFrame to disk
        raw_dir: Directory path to save raw data files

    Returns:
        Normalized OHLCV DataFrame
    """
    downloader = MarketDataDownloader()
    return downloader.download_data(
        symbol=symbol,
        source=source,
        interval=interval,
        limit=limit,
        period=period,
        use_cache=False,
        save_to_disk=to_disk,
        output_directory=raw_dir,
    )
