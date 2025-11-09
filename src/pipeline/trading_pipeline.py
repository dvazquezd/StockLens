"""Main trading analysis pipeline orchestration with OOP architecture."""

import json
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

from config.config import (
    AGENT_MODE, ASSETS_CONFIG, DEFAULT_INTERVAL, DEFAULT_LIMIT, DEFAULT_PERIOD,
    LLM_MODEL, LLM_PROVIDER, PROCESSED_PATH, RAW_PATH
)
from src.agent.agents.factory import AgentFactory
from src.data_ingestion.market_data import MarketDataDownloader
from src.database.market_db import MarketDatabase
from src.features.indicators import TechnicalIndicatorCalculator
from src.signals.signals import TradingSignalGenerator


class TradingAnalysisPipeline:
    """Complete trading analysis pipeline from data ingestion to signal generation."""

    def __init__(self, db_path: str = "data/stocklens.db", use_cache: bool = True):
        """
        Initialize the trading analysis pipeline.

        Args:
            db_path: Path to SQLite database for caching and historical tracking
            use_cache: Whether to use intelligent caching for data downloads
        """
        self.db_path = db_path
        self.use_cache = use_cache
        self.data_downloader = MarketDataDownloader(db_path=db_path)
        self.indicator_calculator = TechnicalIndicatorCalculator()
        self.signal_generator = TradingSignalGenerator()

    def _ensure_directories_exist(self) -> None:
        """Ensure required data directories exist."""
        RAW_PATH.mkdir(parents=True, exist_ok=True)
        PROCESSED_PATH.mkdir(parents=True, exist_ok=True)

    def run_asset_pipeline(
        self,
        symbol: str,
        source: str,
        interval: str,
        limit: Optional[int] = None,
        period: Optional[str] = None,
        save_intermediate: bool = True,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Execute complete pipeline for a single asset.

        Steps:
            1. Downloads raw market data (with intelligent caching)
            2. Calculates technical indicators
            3. Generates trading signals
            4. Saves all data to SQLite database for historical tracking
            5. Optionally saves intermediate datasets to parquet files

        Args:
            symbol: Asset symbol or ticker
            source: Data source identifier
            interval: Time interval for data
            limit: Number of data points (Binance)
            period: Time period (Yahoo)
            save_intermediate: Whether to save intermediate results

        Returns:
            Tuple of (raw_data, indicators_data, signals_data)
        """
        # Download raw market data with intelligent caching
        raw_data = self.data_downloader.download_data(
            symbol=symbol,
            source=source,
            interval=interval,
            limit=limit,
            period=period,
            use_cache=self.use_cache,
            save_to_disk=save_intermediate,
            output_directory=RAW_PATH,
        )

        # Calculate technical indicators
        indicators_data = self.indicator_calculator.calculate_indicators(raw_data)

        # Generate trading signals
        signals_data = self.signal_generator.generate_signals(indicators_data)

        # Save to database for historical tracking
        if self.use_cache:
            with MarketDatabase(self.db_path) as db:
                indicators_saved = db.insert_indicators(indicators_data, symbol, source, interval)
                signals_saved = db.insert_signals(signals_data, symbol, source, interval)
                print(f"💾 {symbol}: Saved {indicators_saved} indicators, {signals_saved} signals to database")

        # Optionally save to parquet files
        if save_intermediate:
            indicators_file = PROCESSED_PATH / f"{symbol}_{interval}_ind.parquet"
            indicators_data.to_parquet(indicators_file, index=False)

            signals_file = PROCESSED_PATH / f"{symbol}_{interval}_signals.parquet"
            signals_data.to_parquet(signals_file, index=False)
            print(f"📄 {symbol}: Saved parquet files")

        return raw_data, indicators_data, signals_data

    def run_analysis_agent(self, mode: Optional[str] = None, provider: Optional[str] = None, model: Optional[str] = None) -> None:
        """
        Execute the appropriate analysis agent using the new OOP architecture.

        Args:
            mode: Agent mode ('local' or 'llm'). If None, uses AGENT_MODE from config
            provider: LLM provider ('anthropic', 'openai'). If None, uses LLM_PROVIDER from config
            model: Model identifier. If None, uses LLM_MODEL from config
        """
        mode = mode or AGENT_MODE

        if mode == "local":
            agent = AgentFactory.create_agent("local")
            agent.analyze_signals(PROCESSED_PATH)

        elif mode == "llm":
            provider = provider or LLM_PROVIDER
            model = model or LLM_MODEL
            agent = AgentFactory.create_agent(provider, model=model)
            agent.analyze_signals(PROCESSED_PATH)

        else:
            raise ValueError(f"Unknown agent mode: {mode}. Use 'local' or 'llm'.")

    def run_complete_pipeline(self) -> None:
        """
        Execute the complete trading analysis pipeline for all configured assets.

        This function:
            - Ensures required directories exist
            - Loads the asset configuration
            - Iterates over each configured asset and runs the pipeline
            - Handles errors gracefully, allowing other assets to continue processing
            - Prints progress, errors, and warnings
            - Runs the analysis agent if at least one asset was successful
        """
        self._ensure_directories_exist()

        # Load asset configuration
        try:
            with ASSETS_CONFIG.open(encoding="utf-8") as f:
                assets_config = json.load(f)
        except FileNotFoundError:
            print(f"Error: Asset configuration file not found: {ASSETS_CONFIG}")
            return
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON in configuration file: {e}")
            return

        if not assets_config:
            print("Warning: No assets configured in the configuration file")
            return

        # Track processing statistics
        total_assets = len(assets_config)
        successful = 0
        failed = 0
        skipped = 0

        # Process each configured asset
        for idx, asset_config in enumerate(assets_config, 1):
            try:
                symbol = asset_config.get("symbol")
                source = asset_config.get("source")

                if not symbol or not source:
                    print(f"\nWarning [{idx}/{total_assets}]: Asset missing 'symbol' or 'source', skipping: {asset_config}")
                    skipped += 1
                    continue

                interval = asset_config.get("interval", DEFAULT_INTERVAL)

                print(f"\n=== [{idx}/{total_assets}] Processing {symbol} ({source}) ===")

                if source == "binance":
                    limit = int(asset_config.get("limit", DEFAULT_LIMIT))
                    self.run_asset_pipeline(
                        symbol=symbol,
                        source=source,
                        interval=interval,
                        limit=limit,
                        save_intermediate=True
                    )
                    successful += 1

                elif source == "yahoo":
                    period = asset_config.get("period", DEFAULT_PERIOD)
                    self.run_asset_pipeline(
                        symbol=symbol,
                        source=source,
                        interval=interval,
                        period=period,
                        save_intermediate=True
                    )
                    successful += 1

                else:
                    print(f"\nWarning [{idx}/{total_assets}]: Unsupported data source '{source}' for {symbol}")
                    skipped += 1
                    continue

            except Exception as e:
                failed += 1
                symbol_info = asset_config.get("symbol", "unknown")
                print(f"\n❌ Error processing {symbol_info}: {type(e).__name__}: {e}")
                print(f"   Continuing with the next asset...")

        # Print summary
        print(f"\n{'='*60}")
        print(f"PROCESSING SUMMARY:")
        print(f"  Total: {total_assets} assets")
        print(f"  ✓ Successful: {successful}")
        print(f"  ✗ Failed: {failed}")
        print(f"  ⊘ Skipped: {skipped}")
        print(f"{'='*60}\n")

        # Only run agent if at least one asset was successful
        if successful > 0:
            try:
                self.run_analysis_agent()
            except Exception as e:
                print(f"\n❌ Error running analysis agent: {type(e).__name__}: {e}")
                print("   Processed data has been saved, but the agent could not execute.")
        else:
            print("No assets were processed successfully. Skipping agent execution.")
