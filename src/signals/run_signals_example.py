import pandas as pd
from config.config import PROCESSED_PATH
from src.signals.rules import make_recommendations

def run_signals(symbol: str = "AAPL", interval: str = "1d"):
    print(f"Ejecutando señales para {symbol} con intervalo {interval}")
    ind_path = PROCESSED_PATH / f"{symbol}_{interval}_ind.parquet"
    if not ind_path.exists():
        raise FileNotFoundError(f"No existe {ind_path}. Genera indicadores primero.")

    df = pd.read_parquet(ind_path)
    recs = make_recommendations(df)

    out_path = PROCESSED_PATH / f"{symbol}_{interval}_signals.parquet"
    recs.to_parquet(out_path, index=False)
    print(f"Señales generadas en {out_path} para {symbol} con intervalo {interval}")
    #print(recs.tail(5))


