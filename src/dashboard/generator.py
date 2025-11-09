"""Dashboard HTML generator for StockLens."""

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional

import pandas as pd
from jinja2 import Environment, FileSystemLoader

from src.database.market_db import MarketDatabase


class DashboardGenerator:
    """Generates HTML dashboard with Zara-inspired minimalist design."""

    def __init__(self, db_path: str = "data/stocklens.db", output_dir: str = "dashboard"):
        """
        Initialize dashboard generator.

        Args:
            db_path: Path to SQLite database
            output_dir: Directory to save generated HTML files
        """
        self.db_path = Path(db_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Setup Jinja2 templates
        template_dir = Path(__file__).parent / "templates"
        self.jinja_env = Environment(loader=FileSystemLoader(str(template_dir)))

        # Copy static files
        self._copy_static_files()

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
            SELECT DISTINCT DATE(timestamp) as date
            FROM signals
            WHERE timestamp >= date('now', ?)
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
                symbol,
                time,
                close,
                rsi_14,
                macd,
                macd_signal,
                adx,
                score,
                recommendation
            FROM signals
            WHERE DATE(timestamp) = ?
            ORDER BY symbol
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
            SELECT DISTINCT symbol FROM signals
            WHERE DATE(timestamp) = ?
        """

        latest_date = dates[0]
        symbols = db.conn.execute(query, (latest_date,)).fetchall()
        total_assets = len(symbols)

        # Count recommendations in last 30 days
        rec_query = """
            SELECT recommendation, COUNT(*) as count
            FROM signals
            WHERE DATE(timestamp) >= date('now', '-30 days')
            GROUP BY recommendation
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
                DATE(timestamp) as date,
                recommendation,
                COUNT(*) as count
            FROM signals
            WHERE DATE(timestamp) >= date('now', '-30 days')
            GROUP BY date, recommendation
            ORDER BY date
        """

        results = db.conn.execute(query).fetchall()

        # Organize data by recommendation type
        data_by_rec = {'buy': {}, 'sell': {}, 'hold': {}}

        for row in results:
            date, rec, count = row
            if rec in data_by_rec:
                data_by_rec[rec][date] = count

        # Create Plotly traces
        traces = []
        colors = {'buy': '#1A7B4F', 'sell': '#C73E1D', 'hold': '#6B6B6B'}

        for rec_type, color in colors.items():
            dates_list = sorted(data_by_rec[rec_type].keys())
            counts = [data_by_rec[rec_type][d] for d in dates_list]

            traces.append({
                'x': dates_list,
                'y': counts,
                'type': 'scatter',
                'mode': 'lines+markers',
                'name': rec_type.upper(),
                'line': {'color': color, 'width': 2},
                'marker': {'size': 6}
            })

        return json.dumps(traces)

    def _prepare_asset_data(
        self,
        db: MarketDatabase,
        signal: Dict,
        target_date: str
    ) -> Dict:
        """Prepare asset data including rationale and chart."""
        # Get rationale from LLM analysis if available
        rationale = self._get_rationale(db, signal['symbol'], target_date)
        signal['rationale'] = rationale

        # Generate price chart
        chart_data = self._generate_asset_chart(db, signal['symbol'])
        signal['chart_data'] = chart_data

        return signal

    def _get_rationale(self, db: MarketDatabase, symbol: str, date: str) -> str:
        """Get analysis rationale for a symbol from agent_runs."""
        # Try to get from agent_runs table
        query = """
            SELECT rationale
            FROM agent_runs
            WHERE symbol = ? AND DATE(run_date) = ?
            ORDER BY run_date DESC
            LIMIT 1
        """

        result = db.conn.execute(query, (symbol, date)).fetchone()

        if result and result[0]:
            return result[0]

        # Default rationale based on indicators
        signal_query = """
            SELECT rsi_14, macd, macd_signal, adx, recommendation
            FROM signals
            WHERE symbol = ? AND DATE(timestamp) = ?
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

    def _generate_asset_chart(self, db: MarketDatabase, symbol: str) -> str:
        """Generate Plotly chart data for asset price history."""
        # Get last 30 days of price data
        query = """
            SELECT time, close
            FROM market_data
            WHERE symbol = ?
            ORDER BY time DESC
            LIMIT 30
        """

        results = db.conn.execute(query, (symbol,)).fetchall()

        if not results:
            return "[]"

        # Reverse to get chronological order
        results = list(reversed(results))

        times = [row[0] for row in results]
        closes = [row[1] for row in results]

        trace = [{
            'x': times,
            'y': closes,
            'type': 'scatter',
            'mode': 'lines',
            'line': {'color': '#1A1A1A', 'width': 1.5},
            'fill': 'tozeroy',
            'fillcolor': 'rgba(26, 26, 26, 0.1)'
        }]

        return json.dumps(trace)
