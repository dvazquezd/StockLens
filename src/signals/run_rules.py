import os
import pandas as pd
from config.config import DATA_DIR
from src.signals.rules import make_recommendations, SignalConfig


def main(symbol: str = "AAPL", interval: str = "1d"):
    path = os.path.join(DATA_DIR, "processed", f"{symbol}_{interval}_ind.parquet")
    if not os.path.exists(path):
        raise FileNotFoundError(f"No existe {path}. Genera primero indicadores.")
    df = pd.read_parquet(path)
    recs = make_recommendations(df, SignalConfig())
    out = os.path.join(DATA_DIR, "processed", f"{symbol}_{interval}_signals.parquet")
    recs.to_parquet(out, index=False)
    print(f"OK → {out}")
    print(recs.tail(5))

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", default="AAPL")
    p.add_argument("--interval", default="1d")
    args = p.parse_args()
    main(args.symbol, args.interval)
