"""Dashboard HTML generator for StockLens."""

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional

import pandas as pd
from jinja2 import Environment, FileSystemLoader

from src.database.market_db import MarketDatabase


def load_assets_config(config_path: Path = None) -> Dict[str, Dict]:
    """
    Load assets configuration from JSON file.

    Args:
        config_path: Path to assets_config.json

    Returns:
        Dictionary mapping symbol to asset config
    """
    if config_path is None:
        config_path = Path("config/assets_config.json")

    if not config_path.exists():
        return {}

    with open(config_path, 'r') as f:
        assets = json.load(f)

    # Convert list to dict keyed by symbol
    return {asset['symbol']: asset for asset in assets}


class DashboardGenerator:
    """Generates HTML dashboard with Zara-inspired minimalist design."""

    def __init__(self, db_path: str = "data/stocklens.db", output_dir: str = "dashboard", assets_config_path: str = None):
        """
        Initialize dashboard generator.

        Args:
            db_path: Path to SQLite database
            output_dir: Directory to save generated HTML files
            assets_config_path: Path to assets_config.json
        """
        self.db_path = Path(db_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Load assets configuration
        config_path = Path(assets_config_path) if assets_config_path else None
        self.assets_config = load_assets_config(config_path)

        # Setup Jinja2 templates
        template_dir = Path(__file__).parent / "templates"
        self.jinja_env = Environment(loader=FileSystemLoader(str(template_dir)))

        # Add custom filters
        self.jinja_env.filters['js_safe'] = self._js_safe_symbol

        # Copy static files
        self._copy_static_files()

    @staticmethod
    def _js_safe_symbol(symbol: str) -> str:
        """
        Convert a symbol to a JavaScript-safe variable name.
        Replaces dots and other invalid characters with underscores.

        Args:
            symbol: Stock symbol (e.g., "FB2A.DE")

        Returns:
            JavaScript-safe name (e.g., "FB2A_DE")
        """
        import re
        # Replace any character that's not alphanumeric or underscore with underscore
        return re.sub(r'[^a-zA-Z0-9_]', '_', symbol)

    def _copy_static_files(self):
        """Copy CSS and JS files to output directory."""
        import shutil

        static_src = Path(__file__).parent / "static"
        static_dst = self.output_dir / "static"

        if static_src.exists():
            shutil.copytree(static_src, static_dst, dirs_exist_ok=True)

    def generate_dashboard(self, days_back: int = 30):
        """
        Generate complete dashboard with current and historical data.

        Args:
            days_back: Number of days of historical data to include
        """
        print(f"\n🎨 Generating dashboard...")

        with MarketDatabase(self.db_path) as db:
            # Get all available dates
            dates = self._get_available_dates(db, days_back)

            if not dates:
                print("⚠️  No data found in database")
                return

            # Generate main dashboard (latest date)
            latest_date = dates[0]
            self._generate_page_for_date(db, latest_date, dates, is_main=True)

            # Generate historical pages
            for date in dates[1:]:
                self._generate_page_for_date(db, date, dates, is_main=False)

        print(f"✓ Dashboard generated: {self.output_dir}/index.html")
        print(f"✓ Historical pages: {len(dates) - 1} files")

    def _get_available_dates(self, db: MarketDatabase, days_back: int) -> List[str]:
        """Get list of dates with available data."""
        query = """
            SELECT DISTINCT DATE(m.timestamp) as date
            FROM signals s
            JOIN market_data m ON s.market_data_id = m.id
            WHERE m.timestamp >= date('now', ?)
            ORDER BY date DESC
        """

        results = db.conn.execute(query, (f'-{days_back} days',)).fetchall()
        return [row[0] for row in results]

    def _generate_page_for_date(
        self,
        db: MarketDatabase,
        target_date: str,
        all_dates: List[str],
        is_main: bool = False
    ):
        """Generate HTML page for a specific date."""
        # Get data for this date
        signals_data = self._get_signals_for_date(db, target_date)

        if not signals_data:
            return

        # Calculate overview stats
        overview_stats = self._calculate_overview_stats(db, all_dates)

        # Generate trend chart data
        trend_data = self._generate_trend_chart(db, all_dates)

        # Prepare asset data with charts
        assets = []
        for signal in signals_data:
            asset_data = self._prepare_asset_data(db, signal, target_date)
            assets.append(asset_data)

        # Calculate portfolio summary
        portfolio_summary = self._calculate_portfolio_summary(assets)

        # Render template
        template = self.jinja_env.get_template("index.html")
        html_content = template.render(
            total_assets=overview_stats['total_assets'],
            buy_count=overview_stats['buy_count'],
            sell_count=overview_stats['sell_count'],
            hold_count=overview_stats['hold_count'],
            current_date=target_date,
            assets=assets,
            historical_dates=all_dates,
            trend_data=trend_data,
            portfolio_summary=portfolio_summary,
            generation_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )

        # Save HTML file
        filename = "index.html" if is_main else f"analysis_{target_date}.html"
        output_path = self.output_dir / filename
        output_path.write_text(html_content, encoding='utf-8')

    def _get_signals_for_date(self, db: MarketDatabase, target_date: str) -> List[Dict]:
        """Get all signals for a specific date."""
        query = """
            SELECT
                m.symbol,
                m.timestamp,
                m.close,
                i.rsi_14,
                i.macd,
                i.macd_signal,
                i.adx,
                s.score,
                s.recommendation
            FROM signals s
            JOIN market_data m ON s.market_data_id = m.id
            LEFT JOIN indicators i ON s.market_data_id = i.market_data_id
            WHERE DATE(m.timestamp) = ?
            ORDER BY m.symbol
        """

        results = db.conn.execute(query, (target_date,)).fetchall()

        signals = []
        for row in results:
            signals.append({
                'symbol': row[0],
                'time': row[1],
                'close': f"{row[2]:.2f}" if row[2] else "N/A",
                'rsi_14': f"{row[3]:.1f}" if row[3] else "N/A",
                'macd': f"{row[4]:.2f}" if row[4] else "N/A",
                'macd_signal': f"{row[5]:.2f}" if row[5] else "N/A",
                'adx': f"{row[6]:.1f}" if row[6] else "N/A",
                'score': row[7] if row[7] is not None else 0,
                'recommendation': row[8] or 'hold'
            })

        return signals

    def _calculate_overview_stats(self, db: MarketDatabase, dates: List[str]) -> Dict:
        """Calculate overview statistics for the last 30 days."""
        if not dates:
            return {'total_assets': 0, 'buy_count': 0, 'sell_count': 0, 'hold_count': 0}

        # Get latest signals for each symbol
        query = """
            SELECT DISTINCT m.symbol
            FROM signals s
            JOIN market_data m ON s.market_data_id = m.id
            WHERE DATE(m.timestamp) = ?
        """

        latest_date = dates[0]
        symbols = db.conn.execute(query, (latest_date,)).fetchall()
        total_assets = len(symbols)

        # Count recommendations in last 30 days
        rec_query = """
            SELECT s.recommendation, COUNT(*) as count
            FROM signals s
            JOIN market_data m ON s.market_data_id = m.id
            WHERE DATE(m.timestamp) >= date('now', '-30 days')
            GROUP BY s.recommendation
        """

        rec_counts = db.conn.execute(rec_query).fetchall()
        rec_dict = {row[0]: row[1] for row in rec_counts}

        return {
            'total_assets': total_assets,
            'buy_count': rec_dict.get('buy', 0),
            'sell_count': rec_dict.get('sell', 0),
            'hold_count': rec_dict.get('hold', 0)
        }

    def _generate_trend_chart(self, db: MarketDatabase, dates: List[str]) -> str:
        """Generate JSON data for 30-day trend chart."""
        if not dates:
            return "[]"

        # Get recommendation counts by date
        query = """
            SELECT
                DATE(m.timestamp) as date,
                s.recommendation,
                COUNT(*) as count
            FROM signals s
            JOIN market_data m ON s.market_data_id = m.id
            WHERE DATE(m.timestamp) >= date('now', '-30 days')
            GROUP BY date, s.recommendation
            ORDER BY date
        """

        results = db.conn.execute(query).fetchall()

        # Organize data by recommendation type
        data_by_rec = {'buy': {}, 'sell': {}, 'hold': {}}

        for row in results:
            date, rec, count = row
            if rec in data_by_rec:
                data_by_rec[rec][date] = count

        # Create Plotly traces with minimalist colors
        traces = []
        colors = {'buy': '#0A8754', 'sell': '#D4423F', 'hold': '#8B8B8B'}

        for rec_type, color in colors.items():
            dates_list = sorted(data_by_rec[rec_type].keys())
            counts = [data_by_rec[rec_type][d] for d in dates_list]

            traces.append({
                'x': dates_list,
                'y': counts,
                'type': 'scatter',
                'mode': 'lines+markers',
                'name': rec_type.upper(),
                'line': {'color': color, 'width': 1.5},
                'marker': {'size': 5, 'color': color}
            })

        return json.dumps(traces)

    def _calculate_portfolio_summary(self, assets: List[Dict]) -> Dict:
        """Calculate portfolio-wide P&L summary."""
        portfolio_assets = [a for a in assets if a.get('in_portfolio', False)]

        if not portfolio_assets:
            return {
                'has_portfolio': False,
                'total_cost_basis': 0,
                'total_current_value': 0,
                'total_pnl_amount': 0,
                'total_pnl_percent': 0,
                'portfolio_count': 0
            }

        total_cost = sum(a.get('cost_basis', 0) for a in portfolio_assets)
        total_value = sum(a.get('current_value', 0) for a in portfolio_assets)
        total_pnl = total_value - total_cost
        total_pnl_pct = (total_pnl / total_cost * 100) if total_cost > 0 else 0

        return {
            'has_portfolio': True,
            'total_cost_basis': total_cost,
            'total_current_value': total_value,
            'total_pnl_amount': total_pnl,
            'total_pnl_percent': total_pnl_pct,
            'portfolio_count': len(portfolio_assets)
        }

    def _prepare_asset_data(
        self,
        db: MarketDatabase,
        signal: Dict,
        target_date: str
    ) -> Dict:
        """Prepare asset data including rationale, chart, and P&L."""
        # Get rationale from LLM analysis if available
        rationale = self._get_rationale(db, signal['symbol'], target_date)
        signal['rationale'] = rationale

        # Add portfolio status and P&L calculations
        symbol = signal['symbol']
        if symbol in self.assets_config:
            asset_config = self.assets_config[symbol]
            signal['in_portfolio'] = asset_config.get('in_portfolio', False)

            # Calculate P&L if in portfolio
            if signal['in_portfolio']:
                purchase_price = asset_config.get('purchase_price')
                shares = asset_config.get('shares', 0)
                purchase_date = asset_config.get('purchase_date')

                if purchase_price and shares:
                    # Get current price from signal
                    try:
                        current_price = float(signal['close'].replace(',', '').replace('$', ''))
                    except (ValueError, AttributeError):
                        current_price = float(signal['close']) if isinstance(signal['close'], (int, float)) else 0

                    # Calculate P&L
                    cost_basis = purchase_price * shares
                    current_value = current_price * shares
                    pnl_amount = current_value - cost_basis
                    pnl_percent = (pnl_amount / cost_basis * 100) if cost_basis > 0 else 0

                    signal['purchase_price'] = purchase_price
                    signal['shares'] = shares
                    signal['purchase_date'] = purchase_date
                    signal['cost_basis'] = cost_basis
                    signal['current_value'] = current_value
                    signal['pnl_amount'] = pnl_amount
                    signal['pnl_percent'] = pnl_percent
                    signal['current_price'] = current_price
        else:
            signal['in_portfolio'] = False

        # Generate price chart (with purchase price line if in portfolio)
        chart_data = self._generate_asset_chart(db, signal['symbol'], signal.get('purchase_price'))
        signal['chart_data'] = chart_data

        return signal

    def _get_rationale(self, db: MarketDatabase, symbol: str, date: str) -> str:
        """Get analysis rationale for a symbol from recommendations."""
        # Try to get from recommendations table
        query = """
            SELECT r.rationale
            FROM recommendations r
            WHERE r.symbol = ? AND DATE(r.created_at) = ?
            ORDER BY r.created_at DESC
            LIMIT 1
        """

        result = db.conn.execute(query, (symbol, date)).fetchone()

        if result and result[0]:
            return result[0]

        # Default rationale based on indicators
        signal_query = """
            SELECT i.rsi_14, i.macd, i.macd_signal, i.adx, s.recommendation
            FROM signals s
            JOIN market_data m ON s.market_data_id = m.id
            LEFT JOIN indicators i ON s.market_data_id = i.market_data_id
            WHERE m.symbol = ? AND DATE(m.timestamp) = ?
            LIMIT 1
        """

        signal_result = db.conn.execute(signal_query, (symbol, date)).fetchone()

        if not signal_result:
            return "Technical analysis pending."

        rsi, macd, macd_sig, adx, rec = signal_result
        reasons = []

        if rec == 'buy':
            reasons.append("Buy signal detected")
        elif rec == 'sell':
            reasons.append("Sell signal detected")
        else:
            reasons.append("Hold position recommended")

        if macd and macd_sig:
            if macd > macd_sig:
                reasons.append("MACD above signal (bullish momentum)")
            else:
                reasons.append("MACD below signal (bearish momentum)")

        if rsi:
            if rsi < 30:
                reasons.append("RSI < 30 (oversold)")
            elif rsi > 70:
                reasons.append("RSI > 70 (overbought)")

        if adx and adx >= 25:
            reasons.append("Strong trend (ADX ≥ 25)")

        return "; ".join(reasons)

    def _generate_asset_chart(self, db: MarketDatabase, symbol: str, purchase_price: float = None) -> str:
        """Generate Plotly chart data for asset price history."""
        # Get last 30 days of price data
        query = """
            SELECT timestamp, close
            FROM market_data
            WHERE symbol = ?
            ORDER BY timestamp DESC
            LIMIT 30
        """

        results = db.conn.execute(query, (symbol,)).fetchall()

        if not results:
            return "[]"

        # Reverse to get chronological order
        results = list(reversed(results))

        times = [row[0] for row in results]
        closes = [row[1] for row in results]

        traces = [{
            'x': times,
            'y': closes,
            'type': 'scatter',
            'mode': 'lines',
            'name': 'Price',
            'line': {'color': '#000000', 'width': 1.2},
            'fill': 'tozeroy',
            'fillcolor': 'rgba(0, 0, 0, 0.03)',
            'showlegend': False
        }]

        # Add purchase price line if available
        if purchase_price:
            traces.append({
                'x': times,
                'y': [purchase_price] * len(times),
                'type': 'scatter',
                'mode': 'lines',
                'name': 'Purchase Price',
                'line': {
                    'color': '#8B8B8B',
                    'width': 1,
                    'dash': 'dash'
                },
                'showlegend': False
            })

        return json.dumps(traces)
