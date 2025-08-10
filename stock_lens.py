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

from src.data_ingestion.market_data import download_market_data
from src.features.indicators import enrich_with_indicators
from src.signals.signals import make_recommendations  # renombra tu módulo si aún es run_signals_example


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
):
    """
    Executes the end-to-end data ingestion, feature engineering, and signal
    generation pipeline for a given asset.

    Steps:
        1. Downloads raw market data for the specified asset from the given source.
        2. Enriches the dataset with calculated technical indicators.
        3. Optionally saves intermediate datasets (raw and enriched).
        4. Generates trading signals based on enriched data.
        5. Saves the final signals dataset.

    Parameters:
        symbol (str): The ticker or symbol of the asset (e.g., 'BTCUSDT', 'AAPL').
        source (str): The data source identifier (e.g., 'binance', 'yahoo').
        interval (str): The time interval for the market data (e.g., '1h', '1d').
        limit (Optional[int]): Number of data points to retrieve (used by Binance).
        period (Optional[str]): Time period for the data (used by Yahoo Finance).
        save_intermediate (bool, optional): Whether to save raw and enriched datasets
            to disk. Defaults to True.

    Returns:
        tuple: A tuple containing:
            - DataFrame: The raw market data.
            - DataFrame: The enriched market data with indicators.
            - DataFrame: The generated trading signals.
    """
    df_raw = download_market_data(
        symbol=symbol,
        source=source,
        interval=interval,
        limit=limit,
        period=period,
        to_disk=save_intermediate,   
        raw_dir=RAW_PATH,             
    )

    df_ind = enrich_with_indicators(df_raw)

    if save_intermediate:
        out_ind = PROCESSED_PATH / f"{symbol}_{interval}_ind.parquet"
        df_ind.to_parquet(out_ind, index=False)
        #print(f"{symbol} descargado y enriquecido {out_ind}")

    df_sig = make_recommendations(df_ind)

    out_sig = PROCESSED_PATH / f"{symbol}_{interval}_signals.parquet"
    df_sig.to_parquet(out_sig, index=False)
    print(f"{symbol} con señales guardado en {out_sig}")
    #print(df_sig.tail(5).to_string(index=False))

    return df_raw, df_ind, df_sig


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
    Orchestrates the execution of the trading data pipeline for all configured assets.

    This function:
        - Ensures required directories exist.
        - Loads the asset configuration from `ASSETS_CONFIG`.
        - Iterates over each configured asset and runs the `pipeline` function
          according to its source type (Binance or Yahoo).
        - Prints progress and warnings for unsupported sources.

    Returns:
        None
    """
    ensure_dirs()
    with ASSETS_CONFIG.open(encoding="utf-8") as f:
        assets = json.load(f)
        
    for a in assets:
        symbol   = a["symbol"]
        source   = a["source"]
        interval = a.get("interval", DEFAULT_INTERVAL)

        if source == "binance":
            limit = int(a.get("limit", DEFAULT_LIMIT))
            print(f"\n=== {symbol} ({source}) ===")
            pipeline(symbol, source, interval, limit=limit, period=None, save_intermediate=True)

        elif source == "yahoo":
            period = a.get("period", DEFAULT_PERIOD)
            print(f"\n=== {symbol} ({source}) ===")
            pipeline(symbol, source, interval, limit=None, period=period, save_intermediate=True)

        else:
            print(f"Aviso: source no soportado: {source}")
            
    run_agent()

if __name__ == "__main__":
    main()
