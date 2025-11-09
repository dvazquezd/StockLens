"""Main entry point for StockLens trading analysis system."""

from __future__ import annotations
import json
from typing import Optional
from src.agent.agent_local import run_agent_local
from src.agent.agent_llm import run_agent_llm

from config.config import (
    ASSETS_CONFIG,
    DEFAULT_INTERVAL,
    DEFAULT_PERIOD,
    DEFAULT_LIMIT,
    PROCESSED_PATH,
    RAW_PATH,
    AGENT_MODE,
    LLM_MODEL,
    LLM_PROVIDER
)

from src.data_ingestion.market_data import download_market_data, download_market_data_cached
from src.features.indicators import enrich_with_indicators
from src.signals.signals import make_recommendations
from src.database.market_db import MarketDatabase
from src.pipeline.trading_pipeline import TradingAnalysisPipeline


def ensure_dirs():
    """
    Ensures that the raw and processed data directories exist.

    This function creates the directories defined in `RAW_PATH` and `PROCESSED_PATH`
    if they do not already exist, including all required parent folders.

    Returns:
        None
    """
    RAW_PATH.mkdir(parents=True, exist_ok=True)
    PROCESSED_PATH.mkdir(parents=True, exist_ok=True)

def pipeline(
    symbol: str,
    source: str,
    interval: str,
    limit: Optional[int] = None,
    period: Optional[str] = None,
    save_intermediate: bool = True,
    use_cache: bool = True,
    db_path: str = "data/stocklens.db",
):
    """
    Executes the end-to-end data ingestion, feature engineering, and signal
    generation pipeline for a given asset. (Backward compatibility wrapper)

    Steps:
        1. Downloads raw market data (with intelligent caching).
        2. Enriches the dataset with calculated technical indicators.
        3. Generates trading signals based on enriched data.
        4. Saves all data to SQLite database for historical tracking.
        5. Optionally saves intermediate datasets to parquet files.

    Parameters:
        symbol (str): The ticker or symbol of the asset (e.g., 'BTCUSDT', 'AAPL').
        source (str): The data source identifier (e.g., 'binance', 'yahoo').
        interval (str): The time interval for the market data (e.g., '1h', '1d').
        limit (Optional[int]): Number of data points to retrieve (used by Binance).
        period (Optional[str]): Time period for the data (used by Yahoo Finance).
        save_intermediate (bool, optional): Whether to save datasets to parquet. Defaults to True.
        use_cache (bool, optional): Whether to use intelligent caching. Defaults to True.
        db_path (str, optional): Path to SQLite database. Defaults to "data/stocklens.db".

    Returns:
        tuple: A tuple containing:
            - DataFrame: The raw market data.
            - DataFrame: The enriched market data with indicators.
            - DataFrame: The generated trading signals.
    """
    # Use the OOP pipeline internally
    pipeline_obj = TradingAnalysisPipeline(db_path=db_path, use_cache=use_cache)
    return pipeline_obj.run_asset_pipeline(
        symbol=symbol,
        source=source,
        interval=interval,
        limit=limit,
        period=period,
        save_intermediate=save_intermediate,
    )


def run_agent():
    """
    Executes the trading or analysis agent according to the configured mode.

    This function checks the global `AGENT_MODE` setting and runs the
    corresponding agent implementation:

        - `"local"`: Runs the locally implemented agent using `run_agent_local`.
        - `"llm"`: Runs the LLM-based agent using `run_agent_llm` with the
          configured `LLM_MODEL` and `LLM_PROVIDER`.

    The processed data path (`PROCESSED_PATH`) is passed to both agent types.

    Raises:
        ValueError: If `AGENT_MODE` is set to an unsupported value.
    """
    if AGENT_MODE == "local":
        run_agent_local(PROCESSED_PATH)
    elif AGENT_MODE == "llm":
        run_agent_llm(PROCESSED_PATH, model=LLM_MODEL, provider=LLM_PROVIDER)
    else:
        print(f"AGENT_MODE desconocido: {AGENT_MODE}. Usa 'local' o 'llm'.")

def main():
    """
    Main entry point for the StockLens trading analysis system.

    Uses the new OOP architecture with TradingAnalysisPipeline for cleaner,
    more maintainable code.
    """
    try:
        # Initialize and run the complete trading analysis pipeline
        pipeline = TradingAnalysisPipeline(
            db_path="data/stocklens.db",
            use_cache=True
        )
        pipeline.run_complete_pipeline()
        print("\n=== Pipeline execution completed successfully ===")

    except Exception as e:
        print("\n=== Pipeline execution failed ===")
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    main()
