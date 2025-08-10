import pandas as pd
from pathlib import Path
import yfinance as yf
from config.config import RAW_PATH


def download_yahoo(symbol: str, interval: str, period: str):
    # Evita el FutureWarning y conserva precios "Close" sin ajuste automático
    df = yf.download(symbol, interval=interval, period=period, auto_adjust=False)
    
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] for col in df.columns]

    if df.empty:
        raise ValueError(f"Yahoo devolvió un DataFrame vacío para {symbol}")

    # Mover índice a columna y normalizar nombres
    df = df.reset_index()
    time_col = "Datetime" if "Datetime" in df.columns else "Date"
    df = df.rename(
        columns={
            time_col: "time",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )

    # Quedarnos con las columnas que usa el pipeline
    keep = ["time", "open", "high", "low", "close", "volume"]
    df = df[keep].sort_values("time").reset_index(drop=True)

    out = RAW_PATH / f"{symbol}_{interval}.parquet"
    df.to_parquet(out, index=False)

    # Evitar carácter no imprimible en Windows cp1252
    print(f"Yahoo guardado: {out} ({len(df)} filas)")


def download_binance(symbol: str, interval: str = "1d", limit: int = 1000) -> Path:
    from src.data_ingestion.binance_client import save_ohlcv
    save_ohlcv(symbol, interval, limit)
    out = RAW_PATH / f"{symbol}_{interval}.parquet"
    print(f"Binance guardado: {out}")
    return out


def download_market_data(symbol: str, source: str, interval:str, limit:str, period:str) -> None:
        if source == "binance":
            download_binance(symbol, interval, limit)
        elif source == "yahoo":
            download_yahoo(symbol, interval, period)
        else:
            print(f"Fuente desconocida: {source}")
