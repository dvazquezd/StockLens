"""Incremental data cache manager using SQLite database."""

from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple
import pandas as pd

from src.database.market_db import MarketDatabase


class DataCache:
    """
    Manages incremental caching of market data.

    Features:
    - Downloads only new data (incremental updates)
    - Merges new data with cached data
    - Handles data gaps intelligently
    - Validates data freshness
    """

    def __init__(self, db_path: Path | str = "data/stocklens.db"):
        """
        Initialize data cache.

        Args:
            db_path: Path to SQLite database
        """
        self.db = MarketDatabase(db_path)

    def get_cached_data(
        self,
        symbol: str,
        source: str,
        interval: str,
        limit: Optional[int] = None
    ) -> Tuple[Optional[pd.DataFrame], Optional[datetime]]:
        """
        Get cached data and determine what needs to be downloaded.

        Args:
            symbol: Asset symbol
            source: Data source
            interval: Time interval
            limit: Desired total number of rows

        Returns:
            Tuple of (cached_dataframe, latest_timestamp)
            - cached_dataframe: None if no cache exists
            - latest_timestamp: None if no cache exists
        """
        latest_timestamp = self.db.get_latest_timestamp(symbol, source, interval)

        if latest_timestamp is None:
            # No cache exists
            return None, None

        # Get cached data
        cached_df = self.db.get_market_data(
            symbol=symbol,
            source=source,
            interval=interval,
            limit=limit
        )

        if cached_df.empty:
            return None, None

        return cached_df, latest_timestamp

    def needs_update(
        self,
        latest_timestamp: Optional[datetime],
        interval: str,
        max_age_hours: int = 24
    ) -> bool:
        """
        Determine if cached data needs updating.

        Args:
            latest_timestamp: Latest timestamp in cache
            interval: Time interval (e.g., '1h', '1d')
            max_age_hours: Maximum age in hours before requiring update

        Returns:
            True if update needed
        """
        if latest_timestamp is None:
            return True

        # Parse interval to determine expected update frequency
        interval_minutes = self._parse_interval_to_minutes(interval)
        expected_delay = timedelta(minutes=interval_minutes * 2)  # Allow 2x interval delay

        # Check if data is stale
        age = datetime.now(latest_timestamp.tzinfo) - latest_timestamp
        max_age = timedelta(hours=max_age_hours)

        return age > expected_delay or age > max_age

    def save_to_cache(
        self,
        df: pd.DataFrame,
        symbol: str,
        source: str,
        interval: str
    ) -> int:
        """
        Save data to cache.

        Args:
            df: DataFrame with OHLCV data
            symbol: Asset symbol
            source: Data source
            interval: Time interval

        Returns:
            Number of rows inserted
        """
        return self.db.insert_market_data(df, symbol, source, interval)

    def merge_with_cache(
        self,
        new_df: pd.DataFrame,
        symbol: str,
        source: str,
        interval: str,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Merge new data with cached data and save to database.

        Args:
            new_df: New DataFrame to merge
            symbol: Asset symbol
            source: Data source
            interval: Time interval
            limit: Maximum number of rows to keep

        Returns:
            Merged DataFrame
        """
        # Get cached data
        cached_df, _ = self.get_cached_data(symbol, source, interval)

        if cached_df is None or cached_df.empty:
            # No cache, save and return new data
            self.save_to_cache(new_df, symbol, source, interval)
            return new_df

        # Merge: concatenate and remove duplicates
        merged_df = pd.concat([cached_df, new_df], ignore_index=True)
        merged_df = merged_df.drop_duplicates(subset=['time'], keep='last')
        merged_df = merged_df.sort_values('time').reset_index(drop=True)

        # Apply limit if specified (keep most recent)
        if limit and len(merged_df) > limit:
            merged_df = merged_df.tail(limit).reset_index(drop=True)

        # Save new data to cache
        self.save_to_cache(new_df, symbol, source, interval)

        return merged_df

    def get_download_params(
        self,
        symbol: str,
        source: str,
        interval: str,
        requested_limit: int
    ) -> Tuple[bool, Optional[int], Optional[datetime]]:
        """
        Determine optimal download parameters for incremental update.

        Args:
            symbol: Asset symbol
            source: Data source
            interval: Time interval
            requested_limit: Total number of rows requested

        Returns:
            Tuple of (use_cache, download_limit, start_date)
            - use_cache: Whether to use cache
            - download_limit: Number of new rows to download
            - start_date: Start date for incremental download
        """
        cached_df, latest_timestamp = self.get_cached_data(symbol, source, interval)

        if cached_df is None or cached_df.empty:
            # No cache: download full limit
            return False, requested_limit, None

        cached_count = len(cached_df)

        if cached_count >= requested_limit:
            # Cache has enough data, check if needs update
            if self.needs_update(latest_timestamp, interval):
                # Download only recent data to update cache
                download_limit = max(100, requested_limit // 10)  # Download 10% or min 100
                return True, download_limit, latest_timestamp
            else:
                # Cache is fresh and sufficient
                return True, 0, latest_timestamp

        else:
            # Cache exists but insufficient, download missing rows
            missing_count = requested_limit - cached_count
            download_limit = int(missing_count * 1.1)  # Download 10% extra for safety
            return True, download_limit, None

    def _parse_interval_to_minutes(self, interval: str) -> int:
        """
        Parse interval string to minutes.

        Args:
            interval: Interval string (e.g., '1h', '1d', '15m')

        Returns:
            Number of minutes
        """
        interval = interval.lower().strip()

        if interval.endswith('m'):
            return int(interval[:-1])
        elif interval.endswith('h'):
            return int(interval[:-1]) * 60
        elif interval.endswith('d'):
            return int(interval[:-1]) * 1440
        elif interval.endswith('w'):
            return int(interval[:-1]) * 10080
        else:
            # Default to daily
            return 1440

    def invalidate_cache(
        self,
        symbol: Optional[str] = None,
        source: Optional[str] = None
    ) -> None:
        """
        Invalidate (clear) cache for specific symbol or all data.

        Args:
            symbol: Optional symbol to clear (clears all if None)
            source: Optional source to clear
        """
        # This would require adding a DELETE method to MarketDatabase
        # For now, just log
        print(f"Cache invalidation requested for symbol={symbol}, source={source}")
        print("Note: Full invalidation not yet implemented")

    def get_cache_stats(self) -> dict:
        """
        Get cache statistics.

        Returns:
            Dictionary with cache statistics
        """
        cursor = self.db.conn.cursor()

        # Count total market data rows
        cursor.execute("SELECT COUNT(*) as count FROM market_data")
        market_data_count = cursor.fetchone()['count']

        # Count unique symbols
        cursor.execute("SELECT COUNT(DISTINCT symbol) as count FROM market_data")
        symbols_count = cursor.fetchone()['count']

        # Get oldest and newest timestamps
        cursor.execute("SELECT MIN(timestamp) as oldest, MAX(timestamp) as newest FROM market_data")
        result = cursor.fetchone()

        return {
            'total_rows': market_data_count,
            'unique_symbols': symbols_count,
            'oldest_data': result['oldest'],
            'newest_data': result['newest'],
            'database_path': str(self.db.db_path),
            'database_size_mb': self.db.db_path.stat().st_size / (1024 * 1024) if self.db.db_path.exists() else 0
        }

    def close(self) -> None:
        """Close database connection."""
        self.db.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
