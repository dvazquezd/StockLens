import os
import pandas as pd
import yfinance as yf
from binance.client import Client
from dotenv import load_dotenv
from datetime import datetime, timezone

# --- Configuración ---
load_dotenv()
DATA_DIR = os.getenv("DATA_DIR", "./data")
os.makedirs(os.path.join(DATA_DIR, "raw"), exist_ok=True)

# --- Yahoo Finance: acciones y ETFs ---
def download_yahoo(symbol: str, interval: str = "1d", period: str = "2y") -> pd.DataFrame:
    """
    Descarga OHLCV desde Yahoo Finance.
    interval: '1d', '1h', '15m', '5m', '1m'
    period: '1y', '2y', '6mo', etc.
    """
    print(f"Descargando {symbol} desde Yahoo Finance...")
    ticker = yf.Ticker(symbol)
    df = ticker.history(interval=interval, period=period)
    if df.empty:
        raise ValueError(f"No se obtuvieron datos para {symbol} de Yahoo Finance.")
    df = df.rename(columns={"Open": "open", "High": "high", "Low": "low",
                            "Close": "close", "Volume": "volume"})
    df["time"] = df.index.tz_convert(timezone.utc)
    df = df[["time", "open", "high", "low", "close", "volume"]].reset_index(drop=True)
    return df

# --- Binance: criptomonedas ---
def download_binance(symbol: str, interval: str = "1d", limit: int = 500) -> pd.DataFrame:
    """
    Descarga OHLCV desde Binance (mercado spot).
    symbol: 'BTCUSDT', 'ETHUSDT', etc.
    interval: '1d', '1h', '15m', '5m', '1m'
    limit: número de velas (máx. 1000 por llamada)
    """
    print(f"Descargando {symbol} desde Binance...")
    client = Client()
    klines = client.get_klines(symbol=symbol, interval=interval, limit=limit)
    if not klines:
        raise ValueError(f"No se obtuvieron datos para {symbol} de Binance.")
    df = pd.DataFrame(klines, columns=[
        "time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "trades",
        "taker_buy_base", "taker_buy_quote", "ignore"
    ])
    df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
    df = df.astype({"open": float, "high": float, "low": float,
                    "close": float, "volume": float})
    df = df[["time", "open", "high", "low", "close", "volume"]]
    return df

# --- Guardar en Parquet ---
def save_parquet(df: pd.DataFrame, filename: str):
    out_path = os.path.join(DATA_DIR, "raw", filename)
    df.to_parquet(out_path, index=False)
    print(f"Datos guardados en {out_path}")

# --- Ejemplo de uso ---
if __name__ == "__main__":
    # Acciones/ETFs
    df_yahoo = download_yahoo("AAPL", interval="1d", period="1y")
    save_parquet(df_yahoo, "AAPL_1d.parquet")

    # Cripto
    df_binance = download_binance("BTCUSDT", interval="1d", limit=365)
    save_parquet(df_binance, "BTCUSDT_1d.parquet")
