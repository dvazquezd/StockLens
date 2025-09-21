"""Main trading analysis pipeline orchestration."""

import json
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

from config.config import (
    AGENT_MODE, ASSETS_CONFIG_PATH, DEFAULT_INTERVAL, DEFAULT_LIMIT, DEFAULT_PERIOD,
    LLM_MODEL, LLM_PROVIDER, PROCESSED_DATA_PATH, RAW_DATA_PATH, AgentMode
)
from src.agent.llm_agent import LLMTradingAgent
from src.agent.local_agent import LocalTradingAgent
from src.data_ingestion.market_data import MarketDataDownloader
from src.features.indicators import TechnicalIndicatorCalculator
from src.signals.signal_generator import TradingSignalGenerator


class TradingAnalysisPipeline:
    """Complete trading analysis pipeline from data ingestion to signal generation."""
    
    def __init__(self):
        self.data_downloader = MarketDataDownloader()
        self.indicator_calculator = TechnicalIndicatorCalculator()
        self.signal_generator = TradingSignalGenerator()
        self.local_agent = LocalTradingAgent()
    
    def _ensure_directories_exist(self) -> None:
        """Ensure required data directories exist."""
        RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)
        PROCESSED_DATA_PATH.mkdir(parents=True, exist_ok=True)
    
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
        # Download raw market data
        raw_data = self.data_downloader.download_data(
            symbol=symbol,
            source=source,
            interval=interval,
            limit=limit,
            period=period,
            save_to_disk=save_intermediate,
            output_directory=RAW_DATA_PATH,
        )
        
        # Calculate technical indicators
        indicators_data = self.indicator_calculator.calculate_indicators(raw_data)
        
        if save_intermediate:
            indicators_file = PROCESSED_DATA_PATH / f"{symbol}_{interval}_indicators.parquet"
            indicators_data.to_parquet(indicators_file, index=False)
        
        # Generate trading signals
        signals_data = self.signal_generator.generate_signals(indicators_data)
        
        signals_file = PROCESSED_DATA_PATH / f"{symbol}_{interval}_signals.parquet"
        signals_data.to_parquet(signals_file, index=False)
        print(f"Signals saved for {symbol} -> {signals_file}")
        
        return raw_data, indicators_data, signals_data
    
    def run_analysis_agent(self, mode: AgentMode) -> None:
        """
        Execute the appropriate analysis agent.
        
        Args:
            mode: Agent mode ('local' or 'llm')
        """
        if mode == "local":
            self.local_agent.analyze_signals(PROCESSED_DATA_PATH)
        
        elif mode == "llm":
            llm_agent = LLMTradingAgent(model=LLM_MODEL, provider=LLM_PROVIDER)
            llm_agent.analyze_signals(PROCESSED_DATA_PATH)
        
        else:
            raise ValueError(f"Unknown agent mode: {mode}. Use 'local' or 'llm'.")
    
    def run_complete_pipeline(self) -> None:
        """Execute the complete trading analysis pipeline for all configured assets."""
        self._ensure_directories_exist()
        
        # Load asset configuration
        try:
            with ASSETS_CONFIG_PATH.open(encoding="utf-8") as f:
                assets_config = json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Assets configuration file not found: {ASSETS_CONFIG_PATH}")
        
        # Process each configured asset
        for asset_config in assets_config:
            symbol = asset_config["symbol"]
            source = asset_config["source"]
            interval = asset_config.get("interval", DEFAULT_INTERVAL)
            
            print(f"\n=== Processing {symbol} ({source}) ===")
            
            try:
                if source == "binance":
                    limit = int(asset_config.get("limit", DEFAULT_LIMIT))
                    self.run_asset_pipeline(
                        symbol=symbol,
                        source=source,
                        interval=interval,
                        limit=limit,
                        save_intermediate=True
                    )
                
                elif source == "yahoo":
                    period = asset_config.get("period", DEFAULT_PERIOD)
                    self.run_asset_pipeline(
                        symbol=symbol,
                        source=source,
                        interval=interval,
                        period=period,
                        save_intermediate=True
                    )
                
                else:
                    print(f"Warning: Unsupported data source: {source}")
                    continue
                    
            except Exception as e:
                print(f"Error processing {symbol}: {e}")
                continue
        
        # Run analysis agent
        try:
            self.run_analysis_agent(AGENT_MODE)
        except Exception as e:
            print(f"Error running analysis agent: {e}")