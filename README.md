# StockLens

StockLens is a Python-based project designed to download, process, and analyze financial market data from multiple sources (Binance, Yahoo Finance), generate technical indicators, compute trading signals, and provide investment recommendations via either a local rules-based agent or an LLM-powered agent.

## Features

- **Multi-source data ingestion**:
  - Binance OHLCV historical data.
  - Yahoo Finance historical data.
- **Technical indicator computation**:
  - RSI, MACD, ATR, ADX, OBV, and more via `pandas_ta`.
- **Signal generation**:
  - Momentum trend, mean reversion, volume-based signals.
- **Agent-based recommendations**:
  - Local agent with predefined rules.
  - LLM agent (OpenAI API) with configurable prompts.

## Installation

```bash
# Clone repository
git clone https://github.com/dvazquezd/StockLens.git
cd StockLens

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Configuration

The project uses environment variables defined in `.env`:

```env
RAW_PATH=./data/raw
PROCESSED_PATH=./data/processed
ASSETS_CONFIG=./config/assets_config.json
DEFAULT_INTERVAL=1d
DEFAULT_LIMIT=1000
DEFAULT_PERIOD=1y
AGENT_MODE=local  # or "llm"
LLM_MODEL=gpt-4o-mini
LLM_PROVIDER=openai
OPENAI_API_KEY=your_api_key_here
```

Additional configuration files:
- `config/assets_config.json` — Defines the assets to download.
- `config/agent_prompt.txt` — LLM prompt instructions.

## Usage

Run the main pipeline:

```bash
python stock_lens.py
```

The pipeline performs:
1. Data download (`data/raw`).
2. Indicator computation (`data/processed`).
3. Signal generation.
4. Agent execution (`local` or `llm`).

### Example Output

```text
=== BTCUSDT (binance) ===
BTCUSDT saved in data/raw/BTCUSDT_1d.parquet (1000 rows)
BTCUSDT with signals saved in data/processed/BTCUSDT_1d_signals.parquet
```

### Agent Modes

#### Local Agent
Rule-based logic using indicators to generate buy/hold/sell recommendations.

#### LLM Agent
Uses OpenAI API to generate recommendations and rationale in structured JSON format.

## Project Structure

```
StockLens/
│
├── config/                 # Configuration files
│   ├── agent_prompt.txt
│   └── assets_config.json
│
├── data/                   # Output folders
│   ├── raw/
│   └── processed/
│
├── src/                    # Source code
│   ├── agent/              # Agents (local + LLM)
│   ├── data_ingestion/     # Market data download
│   ├── features/           # Indicators computation
│   └── signals/            # Trading signals
│
├── stock_lens.py           # Main script
├── requirements.txt
└── README.md
```

## Requirements

Main dependencies:
- `pandas`
- `yfinance`
- `pandas_ta`
- `pyarrow`
- `openai` (for LLM mode)
- `python-dotenv`

See `requirements.txt` for full list.

## License

This project is licensed under the MIT License.
