import json, sys
from config.config import DEFAULT_INTERVAL, DEFAULT_PERIOD, DEFAULT_LIMIT, ASSETS_CONFIG
from src.data_ingestion.market_data import download_market_data
from src.features.run_indicators_example import run_indicators
from src.signals.run_signals_example import run_signals


PY = sys.executable 


def main():
    assets = json.load(ASSETS_CONFIG.open())

    for a in assets:
        symbol = a["symbol"]
        source = a["source"]
        interval = a.get("interval", DEFAULT_INTERVAL)
        period = a.get("period", DEFAULT_PERIOD)
        limit = a.get("limit", DEFAULT_LIMIT)
        print(f"Descargando {symbol} desde {source} con intervalo {interval}, periodo {period}, límite {limit}")
        download_market_data(symbol, source, interval, limit, period)
        run_indicators(symbol, interval)
        run_signals(symbol, interval)

if __name__ == "__main__":
    main()
