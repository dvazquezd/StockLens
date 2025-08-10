import argparse
import pandas as pd
import os
from config.config import RAW_PATH
from binance.client import Client


def get_binance_ohlcv(symbol: str, interval: str, limit: int = 1000) -> pd.DataFrame:
    """Descarga OHLCV de Binance y devuelve un DataFrame."""
    client = Client()  # Sin API key, solo datos públicos
    try:
        klines = client.get_klines(symbol=symbol, interval=interval, limit=limit)
    except Exception as e:
        raise SystemExit(f"Error al descargar datos: {e}")
    
    if not klines:
        raise SystemExit("Binance devolvió un resultado vacío.")
    
    df = pd.DataFrame(klines, columns=[
        "time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base", "taker_buy_quote", "ignore"
    ])
    df["time"] = pd.to_datetime(df["time"], unit="ms")
    df[["open","high","low","close","volume"]] = df[["open","high","low","close","volume"]].astype(float)
    return df[["time","open","high","low","close","volume"]]


def save_ohlcv(symbol: str, interval: str, limit: int = 1000):
    os.makedirs(RAW_PATH, exist_ok=True)
    df = get_binance_ohlcv(symbol, interval, limit)
    file_path = os.path.join(RAW_PATH, f"{symbol}_{interval}.parquet")
    df.to_parquet(file_path, index=False)
    print(f"Guardado: {file_path} ({len(df)} filas)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", type=str, required=True, help="Símbolo, ej: ETHUSDT")
    parser.add_argument("--interval", type=str, default="1d", help="Intervalo, ej: 1d, 1h, 15m")
    parser.add_argument("--limit", type=int, default=1000, help="Número máximo de velas")
    args = parser.parse_args()

    save_ohlcv(args.symbol.upper(), args.interval, args.limit)
