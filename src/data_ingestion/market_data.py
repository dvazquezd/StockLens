import os
import pandas as pd
import yfinance as yf
from pathlib import Path
from config.config import RAW_PATH


def download_yahoo(symbol: str, interval: str = "1d", period: str = "1y"):
    """
    Descarga datos OHLCV de Yahoo Finance y los guarda en /data/raw
    """
    os.makedirs(RAW_PATH, exist_ok=True)
    df = yf.download(symbol, interval=interval, period=period)

    if df.empty:
        raise ValueError(f"No se han podido descargar datos para {symbol} desde Yahoo.")

    df = df.reset_index()
    df = df.rename(columns={
        "Date": "time",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume"
    })
    file_path = RAW_PATH / f"{symbol}_{interval}.parquet"
    df[["time", "open", "high", "low", "close", "volume"]].to_parquet(file_path, index=False)

    print(f"✅ Yahoo guardado: {file_path} ({len(df)} filas)")

def download_binance(symbol: str, interval: str = "1d", limit: int = 1000):
    """
    Descarga datos OHLCV de Binance y los guarda en /data/raw
    """
    from src.data_ingestion.binance_client import save_ohlcv
    save_ohlcv(symbol, interval, limit)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Descargar datos de mercado")
    parser.add_argument("--symbol", type=str, required=True, help="Ticker del activo (ej: BTCUSDT o AAPL)")
    parser.add_argument("--source", type=str, choices=["yahoo", "binance"], required=True, help="Fuente de datos")
    parser.add_argument("--interval", type=str, default="1d", help="Intervalo (ej: 1d, 1h, 15m)")
    parser.add_argument("--period", type=str, default="1y", help="Periodo para Yahoo (ej: 1y, 6mo)")
    parser.add_argument("--limit", type=int, default=1000, help="Número máximo de velas (solo Binance)")

    args = parser.parse_args()

    if args.source == "yahoo":
        download_yahoo(args.symbol, args.interval, args.period)
    elif args.source == "binance":
        download_binance(args.symbol, args.interval, args.limit)
