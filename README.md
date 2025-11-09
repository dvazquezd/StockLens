# StockLens

**Professional Market Analysis Platform with AI-Powered Insights**

StockLens is a sophisticated Python-based financial analysis platform that combines technical analysis, multi-source data ingestion, and AI-powered recommendations to provide comprehensive market insights. Built with modern OOP architecture and featuring an elegant minimalist dashboard.

---

## ✨ Features

### 📊 Data Management
- **Multi-source data ingestion**: Yahoo Finance, Binance API
- **SQLite database**: Efficient caching with incremental updates
- **Historical tracking**: Full audit trail of all analyses and recommendations
- **Parquet storage**: High-performance data serialization

### 📈 Technical Analysis
- **Advanced indicators**: RSI, MACD, ATR, ADX, OBV via `ta` library
- **Smart signal generation**: Momentum, mean reversion, volume-based signals
- **Professional scoring**: Weighted signal aggregation for clear recommendations

### 🤖 AI-Powered Agents
- **Multi-LLM support**: Anthropic Claude, OpenAI GPT
- **OOP architecture**: Extensible factory pattern for easy provider additions
- **Local agent**: Rule-based analysis for offline operation
- **Structured reasoning**: JSON-formatted recommendations with detailed rationale

### 🎨 Professional Dashboard
- **Minimalist design**: Zara-inspired ultra-clean aesthetic
- **Interactive charts**: Plotly-powered visualizations
- **Historical archive**: Browse past analyses and trends
- **Responsive layout**: Works on desktop, tablet, and mobile
- **Real-time updates**: Auto-generated after each pipeline run

---

## 🚀 Quick Start

### Installation

```bash
# Clone repository
git clone https://github.com/dvazquezd/StockLens.git
cd StockLens

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Configuration

Create a `.env` file in the project root:

```env
# Data paths
RAW_PATH=./data/raw
PROCESSED_PATH=./data/processed
ASSETS_CONFIG=./config/assets_config.json

# Data settings
DEFAULT_INTERVAL=1d
DEFAULT_LIMIT=1000
DEFAULT_PERIOD=1y

# Agent configuration
AGENT_MODE=llm  # Options: "llm" | "local"
LLM_PROVIDER=anthropic  # Options: "anthropic" | "openai"
LLM_MODEL=claude-opus-4-1-20250805
PROMPT_PATH=./config/agent_prompt.txt

# API Keys
ANTHROPIC_STOCK_LENS=your_anthropic_key_here
OPENAI_API_KEY=your_openai_key_here  # If using OpenAI
```

Configure your assets in `config/assets_config.json`:

```json
[
  { "symbol": "META",  "source": "yahoo", "interval": "1d", "period": "1y" },
  { "symbol": "NVDA",  "source": "yahoo", "interval": "1d", "period": "1y" }
]
```

### Run Analysis

```bash
python stock_lens.py
```

The pipeline will:
1. ✅ Download market data from configured sources
2. ✅ Calculate technical indicators (RSI, MACD, ADX, etc.)
3. ✅ Generate trading signals with scoring
4. ✅ Save data to SQLite database
5. ✅ Run AI agent analysis
6. ✅ Generate HTML dashboard

### View Dashboard

Open `dashboard/index.html` in your browser to view:
- **Market overview**: KPIs and 30-day trends
- **Portfolio analysis**: Detailed insights per asset
- **Historical archive**: Browse past recommendations

---

## 📁 Project Structure

```
StockLens/
├── config/
│   ├── agent_prompt.txt          # LLM system prompt
│   ├── assets_config.json        # Assets to monitor
│   └── config.py                 # Configuration loader
│
├── src/
│   ├── agent/
│   │   └── agents/               # Multi-LLM agent architecture
│   │       ├── base.py           # Abstract base classes
│   │       ├── factory.py        # Agent factory pattern
│   │       ├── anthropic_agent.py
│   │       ├── openai_agent.py
│   │       └── local_agent.py
│   │
│   ├── dashboard/
│   │   ├── generator.py          # Dashboard HTML generator
│   │   ├── templates/            # Jinja2 templates
│   │   └── static/               # CSS and assets
│   │
│   ├── data_ingestion/
│   │   └── market_data.py        # Yahoo Finance & Binance
│   │
│   ├── database/
│   │   └── market_db.py          # SQLite database manager
│   │
│   ├── features/
│   │   └── indicators.py         # Technical indicators (ta library)
│   │
│   ├── signals/
│   │   └── signals.py            # Trading signal generation
│   │
│   └── pipeline/
│       └── trading_pipeline.py   # Orchestration pipeline
│
├── data/                         # Generated data (gitignored)
│   ├── raw/                      # Raw OHLCV parquet files
│   ├── processed/                # Processed signals
│   └── stocklens.db              # SQLite database
│
├── dashboard/                    # Generated dashboard (gitignored)
│   ├── index.html
│   ├── analysis_*.html
│   └── static/
│
├── stock_lens.py                 # Main entry point
├── requirements.txt
├── .env.example
└── README.md
```

---

## 🔧 Technical Architecture

### OOP Design Principles
- **Factory Pattern**: Easy addition of new LLM providers
- **Dependency Injection**: Configurable components
- **Single Responsibility**: Each class has one clear purpose
- **Abstract Base Classes**: Enforced contracts via interfaces

### Database Schema
- **market_data**: OHLCV time series with source tracking
- **indicators**: Technical indicators linked to market data
- **signals**: Trading signals with recommendations
- **agent_runs**: Execution history and performance metrics
- **recommendations**: AI-generated insights with rationale

### Performance Optimizations
- **Incremental updates**: Only download new data since last run
- **Indexed queries**: Fast database lookups
- **Parquet format**: Efficient columnar storage
- **NaN handling**: Robust data cleaning pipeline

---

## 🤖 Agent Modes

### Local Agent (Rule-Based)
```python
from src.agent.agents import AgentFactory

agent = AgentFactory.create_agent(provider="local")
analysis = agent.analyze_signals(processed_dir)
```

**Features**:
- ✅ No API costs
- ✅ Deterministic results
- ✅ Fast execution
- ✅ Transparent logic

### LLM Agents (AI-Powered)

#### Anthropic Claude
```python
agent = AgentFactory.create_agent(
    provider="anthropic",
    model="claude-opus-4-1-20250805"
)
```

#### OpenAI GPT
```python
agent = AgentFactory.create_agent(
    provider="openai",
    model="gpt-4o-mini"
)
```

**Features**:
- ✅ Sophisticated reasoning
- ✅ Natural language explanations
- ✅ Contextual analysis
- ✅ Adaptive strategies

---

## 📊 Dashboard Features

### Overview Section
- Total assets monitored
- Buy/Sell/Hold signal counts (30-day)
- Interactive trend charts
- Performance metrics

### Portfolio Analysis
- Asset-specific cards with recommendations
- Technical indicator displays
- Mini price charts (30-day history)
- AI-generated rationale

### Historical Archive
- Date-based navigation
- Past analysis retrieval
- Trend comparison
- Performance tracking

---

## 🛠️ Development

### Adding a New LLM Provider

1. Create new agent class in `src/agent/agents/`:

```python
from src.agent.agents.llm_base import LLMAgent

class MyCustomAgent(LLMAgent):
    def _call_llm(self, prompt: str) -> str:
        # Your LLM API call here
        pass
```

2. Register in factory (`src/agent/agents/factory.py`):

```python
elif provider == "custom":
    return MyCustomAgent(model=model, api_key=api_key)
```

3. Use it:

```bash
LLM_PROVIDER=custom python stock_lens.py
```

### Testing

```bash
# Run tests (when available)
pytest

# Run specific test file
pytest tests/test_indicators.py

# Check coverage
pytest --cov=src
```

---

## 📦 Dependencies

| Package | Purpose |
|---------|---------|
| `pandas>=2.2` | Data manipulation |
| `ta>=0.11.0` | Technical indicators |
| `yfinance>=0.2.40` | Yahoo Finance data |
| `anthropic>=0.68.0` | Claude API |
| `openai>=1.40.0` | OpenAI GPT API |
| `jinja2>=3.1.0` | HTML templating |
| `plotly>=5.0` | Interactive charts |
| `pyarrow>=16.0` | Parquet files |

See `requirements.txt` for complete list.

---

## 🗺️ Roadmap

### Completed ✅
- [x] Multi-source data ingestion
- [x] Technical indicator calculation
- [x] SQLite database with caching
- [x] Multi-LLM agent architecture
- [x] Professional HTML dashboard
- [x] Historical analysis archive

### In Progress 🚧
- [ ] Real-time WebSocket feeds
- [ ] Email/Slack notifications
- [ ] Portfolio backtesting engine
- [ ] Risk management module

### Planned 📋
- [ ] ML-based signal optimization
- [ ] Sentiment analysis integration
- [ ] Multi-timeframe analysis
- [ ] Custom indicator builder
- [ ] API for external integrations

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

---

## 📧 Contact

**David Vázquez** - [@dvazquezd](https://github.com/dvazquezd)

Project Link: [https://github.com/dvazquezd/StockLens](https://github.com/dvazquezd/StockLens)

---

## 🙏 Acknowledgments

- [yfinance](https://github.com/ranaroussi/yfinance) for Yahoo Finance data
- [ta](https://github.com/bukosabino/ta) for technical analysis indicators
- [Anthropic](https://www.anthropic.com/) for Claude AI
- [OpenAI](https://openai.com/) for GPT models
- [Plotly](https://plotly.com/) for interactive charts

---

<div align="center">
  <strong>Built with ❤️ for financial analysis</strong>
</div>
