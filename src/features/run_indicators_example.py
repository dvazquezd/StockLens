import os
import pandas as pd
from config.config import DATA_DIR

from src.features.indicators import enrich_with_indicators


def main(symbol: str = "AAPL", interval: str = "1d"):
    raw_path = os.path.join(DATA_DIR, "raw", f"{symbol}_{interval}.parquet")
    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"No existe {raw_path}. Descarga primero el OHLCV.")
    df = pd.read_parquet(raw_path)
    enriched = enrich_with_indicators(df)
    out_path = os.path.join(DATA_DIR, "processed", f"{symbol}_{interval}_ind.parquet")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    enriched.to_parquet(out_path, index=False)
    print(f"OK → {out_path}")
    print(enriched.tail(3)[["time","close","rsi_14","macd","macd_signal","atr_14","adx","obv"]])

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", default="AAPL")
    p.add_argument("--interval", default="1d")
    args = p.parse_args()
    main(args.symbol, args.interval)
