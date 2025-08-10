import pandas as pd
from config.config import RAW_PATH, PROCESSED_PATH
from src.features.indicators import enrich_with_indicators

def run_indicators(symbol: str = "AAPL", interval: str = "1d"):
    print(f"Ejecutando indicadores para {symbol} con intervalo {interval}")
    raw_path = RAW_PATH / f"{symbol}_{interval}.parquet"
    if not raw_path.exists():
        raise FileNotFoundError(f"No existe {raw_path}. Descarga primero el OHLCV.")

    df = pd.read_parquet(raw_path)
    enriched = enrich_with_indicators(df)

    out_path = PROCESSED_PATH / f"{symbol}_{interval}_ind.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    enriched.to_parquet(out_path, index=False)

    print(f"Indicadores guradados en {out_path} para {symbol} con intervalo {interval}")
    #print(enriched.tail(3)[["time","close","rsi_14","macd","macd_signal","atr_14","adx","obv"]])