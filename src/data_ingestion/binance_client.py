# src/data_ingestion/binance_client.py
from __future__ import annotations
import os
import pandas as pd
from binance.client import Client

def _get_client() -> Client:
    # Para datos públicos no necesitas API key, pero si las tienes se usan.
    key = os.getenv("BINANCE_API_KEY")
    sec = os.getenv("BINANCE_API_SECRET")
    return Client(api_key=key, api_secret=sec)

def download_ohlcv(symbol: str, interval: str, limit: int = 1000) -> pd.DataFrame:
    """
    Descarga OHLCV de Binance y devuelve un DataFrame con columnas:
    time, open, high, low, close, volume
    """
    client = _get_client()
    # python-binance acepta '1d', '1h', etc. directamente
    kl = client.get_klines(symbol=symbol, interval=interval, limit=limit)

    cols = [
        "open_time","open","high","low","close","volume",
        "close_time","quote_asset_volume","number_of_trades",
        "taker_buy_base","taker_buy_quote","ignore",
    ]
    df = pd.DataFrame(kl, columns=cols)

    # Tipos + timestamp
    num_cols = ["open","high","low","close","volume"]
    df[num_cols] = df[num_cols].astype(float)
    df["time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)

    # Estandarizar a las columnas mínimas requeridas
    out = df[["time","open","high","low","close","volume"]].sort_values("time").reset_index(drop=True)
    return out
