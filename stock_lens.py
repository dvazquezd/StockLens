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
    generation pipeline for a given asset.

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
    # Download data with caching
    if use_cache:
        df_raw = download_market_data_cached(
            symbol=symbol,
            source=source,
            interval=interval,
            limit=limit,
            period=period,
            use_cache=True,
            db_path=db_path,
        )
    else:
        df_raw = download_market_data(
            symbol=symbol,
            source=source,
            interval=interval,
            limit=limit,
            period=period,
            to_disk=save_intermediate,
            raw_dir=RAW_PATH,
        )

    # Calculate indicators
    df_ind = enrich_with_indicators(df_raw)

    # Generate signals
    df_sig = make_recommendations(df_ind)

    # Save to database
    if use_cache:
        with MarketDatabase(db_path) as db:
            # Indicators already have 'time' column from df_raw
            indicators_saved = db.insert_indicators(df_ind, symbol, source, interval)
            signals_saved = db.insert_signals(df_sig, symbol, source, interval)
            print(f"💾 {symbol}: Saved {indicators_saved} indicators, {signals_saved} signals to database")

    # Optionally save to parquet files
    if save_intermediate:
        out_ind = PROCESSED_PATH / f"{symbol}_{interval}_ind.parquet"
        df_ind.to_parquet(out_ind, index=False)

        out_sig = PROCESSED_PATH / f"{symbol}_{interval}_signals.parquet"
        df_sig.to_parquet(out_sig, index=False)
        print(f"📄 {symbol}: Saved parquet files")

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
        - Handles errors gracefully, allowing other assets to continue processing
          even if one fails.
        - Prints progress, errors, and warnings.

    Returns:
        None
    """
    ensure_dirs()

    try:
        with ASSETS_CONFIG.open(encoding="utf-8") as f:
            assets = json.load(f)
    except FileNotFoundError:
        print(f"Error: No se encontró el archivo de configuración: {ASSETS_CONFIG}")
        return
    except json.JSONDecodeError as e:
        print(f"Error: El archivo de configuración no es un JSON válido: {e}")
        return

    if not assets:
        print("Aviso: No hay assets configurados en el archivo de configuración")
        return

    # Track processing statistics
    total_assets = len(assets)
    successful = 0
    failed = 0
    skipped = 0

    for idx, a in enumerate(assets, 1):
        try:
            symbol = a.get("symbol")
            source = a.get("source")

            if not symbol or not source:
                print(f"\nAviso [{idx}/{total_assets}]: Asset sin 'symbol' o 'source', saltando: {a}")
                skipped += 1
                continue

            interval = a.get("interval", DEFAULT_INTERVAL)

            if source == "binance":
                limit = int(a.get("limit", DEFAULT_LIMIT))
                print(f"\n=== [{idx}/{total_assets}] {symbol} ({source}) ===")
                pipeline(symbol, source, interval, limit=limit, period=None, save_intermediate=True)
                successful += 1

            elif source == "yahoo":
                period = a.get("period", DEFAULT_PERIOD)
                print(f"\n=== [{idx}/{total_assets}] {symbol} ({source}) ===")
                pipeline(symbol, source, interval, limit=None, period=period, save_intermediate=True)
                successful += 1

            else:
                print(f"\nAviso [{idx}/{total_assets}]: source no soportado '{source}' para {symbol}")
                skipped += 1

        except Exception as e:
            failed += 1
            symbol_info = a.get("symbol", "unknown")
            print(f"\n❌ Error procesando {symbol_info}: {type(e).__name__}: {e}")
            print(f"   Continuando con el siguiente asset...")

    # Print summary
    print(f"\n{'='*60}")
    print(f"RESUMEN DE PROCESAMIENTO:")
    print(f"  Total: {total_assets} assets")
    print(f"  ✓ Exitosos: {successful}")
    print(f"  ✗ Fallidos: {failed}")
    print(f"  ⊘ Saltados: {skipped}")
    print(f"{'='*60}\n")

    # Only run agent if at least one asset was successful
    if successful > 0:
        try:
            run_agent()
        except Exception as e:
            print(f"\n❌ Error ejecutando agente: {type(e).__name__}: {e}")
            print("   Los datos procesados están guardados, pero el agente no pudo ejecutarse.")
    else:
        print("No se procesó ningún asset exitosamente. Saltando ejecución del agente.")

if __name__ == "__main__":
    main()
