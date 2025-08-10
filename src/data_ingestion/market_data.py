from __future__ import annotations
from pathlib import Path
import pandas as pd

def _normalize_df_yahoo(df: pd.DataFrame) -> pd.DataFrame:
    """    
    Normalizes Yahoo Finance OHLCV data to a consistent column format.

    This function:
        - Flattens MultiIndex columns if present.
        - Moves the `Date` or `Datetime` index into a column if necessary.
        - Drops the `Adj Close` column if both `Close` and `Adj Close` exist.
        - Renames columns to the standard lowercase OHLCV format:
          `time`, `open`, `high`, `low`, `close`, `volume`.
        - Removes duplicate columns if present.
        - Sorts the dataset by `time` and resets the index.

    Parameters:
        df (pd.DataFrame): The raw DataFrame returned by Yahoo Finance.

    Returns:
        pd.DataFrame: The normalized OHLCV DataFrame containing at least
        `time` and `close`.

    Raises:
        ValueError: If essential columns (`time` and `close`) are missing after normalization.
    """
    # Aplanar MultiIndex si lo hay
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    # Pasar índice Date/Datetime a columna
    if not ("Date" in df.columns or "Datetime" in df.columns):
        if isinstance(df.index, pd.DatetimeIndex) or df.index.name in ("Date", "Datetime"):
            df = df.reset_index()

    # Si existen ambas columnas de cierre, nos quedamos con Close
    if "Close" in df.columns and "Adj Close" in df.columns:
        df = df.drop(columns=["Adj Close"])

    # Renombrar a OHLCV estándar
    rename = {
        "Date": "time", "Datetime": "time",
        "Open": "open", "High": "high", "Low": "low",
        "Close": "close", "Adj Close": "close",
        "Volume": "volume"
    }
    df = df.rename(columns=rename)

    keep = [c for c in ["time", "open", "high", "low", "close", "volume"] if c in df.columns]
    if "time" not in keep or "close" not in keep:
        raise ValueError(f"Yahoo: columnas insuficientes tras normalizar: {df.columns.tolist()}")

    # Eliminar duplicados por si acaso
    df = df.loc[:, ~df.columns.duplicated()]

    return df[keep].sort_values("time").reset_index(drop=True)


def _normalize_df_binance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates and sorts Binance OHLCV data.

    This function ensures that the Binance DataFrame contains all required
    columns (`time`, `open`, `high`, `low`, `close`, `volume`), sorts by `time`,
    and resets the index.

    Parameters:
        df (pd.DataFrame): The raw Binance OHLCV DataFrame.

    Returns:
        pd.DataFrame: The validated and sorted Binance OHLCV DataFrame.

    Raises:
        ValueError: If one or more required OHLCV columns are missing.
    """
    # Asumimos que ya viene con columnas time/open/high/low/close/volume
    need = ["time","open","high","low","close","volume"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise ValueError(f"Binance: faltan columnas {missing}")
    return df.sort_values("time").reset_index(drop=True)

def download_market_data(
    symbol: str,
    source: str,              # "binance" | "yahoo"
    interval: str,
    limit: int | None = None,
    period: str | None = None,
    to_disk: bool = True,
    raw_dir: Path | None = None,
) -> pd.DataFrame:
    """
   Downloads market OHLCV data from Binance or Yahoo Finance, normalizes it,
    and optionally saves it to disk.

    For Binance:
        - Requires `limit` to specify the number of data points.
        - Data is fetched via `download_ohlcv` and validated with `_normalize_df_binance`.

    For Yahoo:
        - Requires `period` to specify the time range (e.g., '1y', '6mo').
        - Data is fetched via `yfinance.download` and normalized with `_normalize_df_yahoo`.

    Parameters:
        symbol (str): The asset ticker or trading pair (e.g., 'BTCUSDT', 'AAPL').
        source (str): Data source identifier, either `"binance"` or `"yahoo"`.
        interval (str): Time interval between data points (e.g., '1h', '1d').
        limit (int | None): Number of data points to fetch (Binance only).
        period (str | None): Historical period to fetch (Yahoo only).
        to_disk (bool, optional): Whether to save the normalized DataFrame to disk. Defaults to True.
        raw_dir (Path | None, optional): Directory path to save raw data files. Defaults to `"data/raw"`.

    Returns:
        pd.DataFrame: A normalized OHLCV DataFrame with columns:
        `time`, `open`, `high`, `low`, `close`, `volume`.

    Raises:
        ValueError: If required parameters are missing for the chosen source
            or if the source is unsupported.
    """
    if source == "binance":
        from src.data_ingestion.binance_client import download_ohlcv  # función que te devuelva df
        if limit is None:
            raise ValueError("Para binance, 'limit' es obligatorio.")
        df = download_ohlcv(symbol=symbol, interval=interval, limit=limit)
        df = _normalize_df_binance(df)

    elif source == "yahoo":
        import yfinance as yf
        if period is None:
            raise ValueError("Para yahoo, 'period' es obligatorio.")
        df = yf.download(symbol, interval=interval, period=period, auto_adjust=False)
        if df.empty:
            raise ValueError(f"Yahoo devolvió vacío para {symbol}")
        df = _normalize_df_yahoo(df)

    else:
        raise ValueError(f"source no soportado: {source}")

    if to_disk:
        out_dir = raw_dir or Path("data/raw")
        out_dir.mkdir(parents=True, exist_ok=True)
        out = out_dir / f"{symbol}_{interval}.parquet"
        df.to_parquet(out, index=False)
        print(f"{symbol} guardado en {out} ({len(df)} filas)")

    return df
