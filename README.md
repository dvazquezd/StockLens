# StockLens
Agente de investigación de inversiones (acciones, ETFs y cripto). Fase 1: ingesta con Finnhub y guardado en Parquet.

## Requisitos
- Python 3.10–3.12
- Cuenta Finnhub (API key)

## Instalación
```bash
python -m venv .venv
# Win: .venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
# Edita .env y añade FINNHUB_API_KEY

## Uso rápido
```bash
python -m src.data_ingestion.finnhub_client --symbol AAPL --resolution D --days 365


**`docs/data_ingestion.md`**
```markdown
# Ingesta Finnhub
Endpoint: `/stock/candle` (OHLCV). Resoluciones: 1,5,15,30,60,D,W,M.
Parámetros: `symbol`, `resolution`, `from`, `to` (UNIX s).
Frecuencia: EOD 1 vez/día; intradía 1–5 min con backoff.
Almacenamiento: Parquet (`time, open, high, low, close, volume`) en `data/raw/`.
