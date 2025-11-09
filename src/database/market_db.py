"""SQLite database manager for market data caching and history tracking."""

from __future__ import annotations
import sqlite3
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Any
import pandas as pd


class MarketDatabase:
    """
    Manages SQLite database for market data caching and analysis history.

    Features:
    - Incremental data caching (download only new data)
    - Historical agent run tracking
    - Performance metrics storage
    - Efficient queries with indexes
    """

    def __init__(self, db_path: Path | str = "data/stocklens.db"):
        """
        Initialize database connection and create tables if needed.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row  # Enable column access by name
        self._create_tables()

    def _create_tables(self) -> None:
        """Create database schema if tables don't exist."""
        cursor = self.conn.cursor()

        # Table 1: Raw market data (OHLCV)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS market_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                source TEXT NOT NULL,
                interval TEXT NOT NULL,
                timestamp DATETIME NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, source, interval, timestamp)
            )
        """)

        # Table 2: Technical indicators
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS indicators (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_data_id INTEGER NOT NULL,
                rsi_14 REAL,
                macd REAL,
                macd_signal REAL,
                atr_14 REAL,
                adx REAL,
                obv REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (market_data_id) REFERENCES market_data(id),
                UNIQUE(market_data_id)
            )
        """)

        # Table 3: Trading signals
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_data_id INTEGER NOT NULL,
                sig_momentum_trend INTEGER DEFAULT 0,
                sig_mean_reversion INTEGER DEFAULT 0,
                sig_volume INTEGER DEFAULT 0,
                score INTEGER DEFAULT 0,
                recommendation TEXT CHECK(recommendation IN ('buy', 'sell', 'hold')),
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (market_data_id) REFERENCES market_data(id),
                UNIQUE(market_data_id)
            )
        """)

        # Table 4: Agent execution runs
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agent_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_timestamp DATETIME NOT NULL,
                agent_type TEXT NOT NULL CHECK(agent_type IN ('local', 'llm')),
                llm_provider TEXT,
                llm_model TEXT,
                assets_processed INTEGER DEFAULT 0,
                assets_failed INTEGER DEFAULT 0,
                execution_time_seconds REAL,
                status TEXT CHECK(status IN ('success', 'partial', 'failed')),
                error_message TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Table 5: Agent recommendations
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS recommendations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_run_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                recommendation TEXT NOT NULL CHECK(recommendation IN ('buy', 'sell', 'hold')),
                rationale TEXT,
                portfolio_analysis TEXT,
                confidence_score REAL,
                price_at_recommendation REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (agent_run_id) REFERENCES agent_runs(id)
            )
        """)

        # Add portfolio_analysis column if it doesn't exist (migration)
        try:
            cursor.execute("SELECT portfolio_analysis FROM recommendations LIMIT 1")
        except:
            cursor.execute("ALTER TABLE recommendations ADD COLUMN portfolio_analysis TEXT")
            self.conn.commit()

        # Create indexes for fast queries
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_market_data_symbol_time
            ON market_data(symbol, timestamp DESC)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_market_data_source
            ON market_data(source, symbol, interval)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_agent_runs_timestamp
            ON agent_runs(run_timestamp DESC)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_recommendations_symbol
            ON recommendations(symbol, created_at DESC)
        """)

        self.conn.commit()

    def get_latest_timestamp(self, symbol: str, source: str, interval: str) -> Optional[datetime]:
        """
        Get the latest timestamp for a given symbol/source/interval.

        Args:
            symbol: Asset symbol (e.g., 'BTCUSDT')
            source: Data source ('binance', 'yahoo')
            interval: Time interval ('1h', '1d', etc.)

        Returns:
            Latest timestamp or None if no data exists
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT MAX(timestamp) as latest
            FROM market_data
            WHERE symbol = ? AND source = ? AND interval = ?
        """, (symbol, source, interval))

        result = cursor.fetchone()
        if result and result['latest']:
            return datetime.fromisoformat(result['latest'])
        return None

    def insert_market_data(self, df: pd.DataFrame, symbol: str, source: str, interval: str) -> int:
        """
        Insert market data into database (OHLCV).

        Args:
            df: DataFrame with columns: time, open, high, low, close, volume
            symbol: Asset symbol
            source: Data source
            interval: Time interval

        Returns:
            Number of rows inserted
        """
        cursor = self.conn.cursor()
        inserted_count = 0

        for _, row in df.iterrows():
            try:
                cursor.execute("""
                    INSERT OR IGNORE INTO market_data
                    (symbol, source, interval, timestamp, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    symbol, source, interval,
                    row['time'].isoformat() if hasattr(row['time'], 'isoformat') else str(row['time']),
                    float(row['open']), float(row['high']), float(row['low']),
                    float(row['close']), float(row['volume'])
                ))
                inserted_count += cursor.rowcount
            except sqlite3.IntegrityError:
                # Skip duplicates
                continue

        self.conn.commit()
        return inserted_count

    def get_market_data(
        self,
        symbol: str,
        source: str,
        interval: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Retrieve market data from database.

        Args:
            symbol: Asset symbol
            source: Data source
            interval: Time interval
            start_date: Optional start date filter
            end_date: Optional end date filter
            limit: Optional limit on number of rows

        Returns:
            DataFrame with market data
        """
        query = """
            SELECT timestamp as time, open, high, low, close, volume
            FROM market_data
            WHERE symbol = ? AND source = ? AND interval = ?
        """
        params: List[Any] = [symbol, source, interval]

        if start_date:
            query += " AND timestamp >= ?"
            params.append(start_date.isoformat())

        if end_date:
            query += " AND timestamp <= ?"
            params.append(end_date.isoformat())

        query += " ORDER BY timestamp ASC"

        if limit:
            query += f" LIMIT {limit}"

        df = pd.read_sql_query(query, self.conn, params=params)

        if not df.empty:
            df['time'] = pd.to_datetime(df['time'])

        return df

    def insert_indicators(self, df: pd.DataFrame, symbol: str, source: str, interval: str) -> int:
        """
        Insert technical indicators linked to market data.

        Args:
            df: DataFrame with indicators (must include 'time' column)
            symbol: Asset symbol
            source: Data source
            interval: Time interval

        Returns:
            Number of rows inserted
        """
        cursor = self.conn.cursor()
        inserted_count = 0

        for _, row in df.iterrows():
            # Get market_data_id for this timestamp
            cursor.execute("""
                SELECT id FROM market_data
                WHERE symbol = ? AND source = ? AND interval = ? AND timestamp = ?
            """, (symbol, source, interval,
                  row['time'].isoformat() if hasattr(row['time'], 'isoformat') else str(row['time'])))

            result = cursor.fetchone()
            if result:
                market_data_id = result['id']

                try:
                    cursor.execute("""
                        INSERT OR REPLACE INTO indicators
                        (market_data_id, rsi_14, macd, macd_signal, atr_14, adx, obv)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        market_data_id,
                        float(row.get('rsi_14', 0)) if pd.notna(row.get('rsi_14')) else None,
                        float(row.get('macd', 0)) if pd.notna(row.get('macd')) else None,
                        float(row.get('macd_signal', 0)) if pd.notna(row.get('macd_signal')) else None,
                        float(row.get('atr_14', 0)) if pd.notna(row.get('atr_14')) else None,
                        float(row.get('adx', 0)) if pd.notna(row.get('adx')) else None,
                        float(row.get('obv', 0)) if pd.notna(row.get('obv')) else None,
                    ))
                    inserted_count += 1
                except sqlite3.Error as e:
                    print(f"Error inserting indicators for {symbol} at {row['time']}: {e}")
                    continue

        self.conn.commit()
        return inserted_count

    def insert_signals(self, df: pd.DataFrame, symbol: str, source: str, interval: str) -> int:
        """
        Insert trading signals linked to market data.

        Args:
            df: DataFrame with signals (must include 'time' column)
            symbol: Asset symbol
            source: Data source
            interval: Time interval

        Returns:
            Number of rows inserted
        """
        cursor = self.conn.cursor()
        inserted_count = 0

        for _, row in df.iterrows():
            # Get market_data_id
            cursor.execute("""
                SELECT id FROM market_data
                WHERE symbol = ? AND source = ? AND interval = ? AND timestamp = ?
            """, (symbol, source, interval,
                  row['time'].isoformat() if hasattr(row['time'], 'isoformat') else str(row['time'])))

            result = cursor.fetchone()
            if result:
                market_data_id = result['id']

                try:
                    cursor.execute("""
                        INSERT OR REPLACE INTO signals
                        (market_data_id, sig_momentum_trend, sig_mean_reversion,
                         sig_volume, score, recommendation)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        market_data_id,
                        int(row.get('sig_momentum_trend', 0)),
                        int(row.get('sig_mean_reversion', 0)),
                        int(row.get('sig_volume', 0)),
                        int(row.get('score', 0)),
                        str(row.get('recommendation', 'hold'))
                    ))
                    inserted_count += 1
                except sqlite3.Error as e:
                    print(f"Error inserting signals for {symbol} at {row['time']}: {e}")
                    continue

        self.conn.commit()
        return inserted_count

    def create_agent_run(
        self,
        agent_type: str,
        assets_processed: int = 0,
        assets_failed: int = 0,
        execution_time: float = 0.0,
        status: str = 'success',
        llm_provider: Optional[str] = None,
        llm_model: Optional[str] = None,
        error_message: Optional[str] = None
    ) -> int:
        """
        Create a new agent run record.

        Args:
            agent_type: Type of agent ('local' or 'llm')
            assets_processed: Number of assets successfully processed
            assets_failed: Number of assets that failed
            execution_time: Execution time in seconds
            status: Run status ('success', 'partial', 'failed')
            llm_provider: LLM provider if agent_type is 'llm'
            llm_model: LLM model if agent_type is 'llm'
            error_message: Error message if status is 'failed'

        Returns:
            ID of created agent run
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO agent_runs
            (run_timestamp, agent_type, llm_provider, llm_model, assets_processed,
             assets_failed, execution_time_seconds, status, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            datetime.now().isoformat(),
            agent_type,
            llm_provider,
            llm_model,
            assets_processed,
            assets_failed,
            execution_time,
            status,
            error_message
        ))

        self.conn.commit()
        return cursor.lastrowid

    def insert_recommendation(
        self,
        agent_run_id: int,
        symbol: str,
        recommendation: str,
        rationale: str,
        price_at_recommendation: Optional[float] = None,
        confidence_score: Optional[float] = None,
        portfolio_analysis: Optional[str] = None
    ) -> int:
        """
        Insert agent recommendation.

        Args:
            agent_run_id: ID of the agent run
            symbol: Asset symbol
            recommendation: Buy/sell/hold recommendation
            rationale: Reasoning for recommendation
            price_at_recommendation: Price at time of recommendation
            confidence_score: Confidence score (0-1)
            portfolio_analysis: Detailed portfolio position analysis (optional)

        Returns:
            ID of created recommendation
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO recommendations
            (agent_run_id, symbol, recommendation, rationale, portfolio_analysis,
             price_at_recommendation, confidence_score)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            agent_run_id,
            symbol,
            recommendation,
            rationale,
            portfolio_analysis,
            price_at_recommendation,
            confidence_score
        ))

        self.conn.commit()
        return cursor.lastrowid

    def get_recommendation_history(
        self,
        symbol: Optional[str] = None,
        limit: int = 100
    ) -> pd.DataFrame:
        """
        Get recommendation history.

        Args:
            symbol: Optional symbol filter
            limit: Maximum number of recommendations

        Returns:
            DataFrame with recommendation history
        """
        query = """
            SELECT
                r.created_at,
                r.symbol,
                r.recommendation,
                r.rationale,
                r.price_at_recommendation,
                r.confidence_score,
                ar.agent_type,
                ar.llm_provider,
                ar.llm_model
            FROM recommendations r
            JOIN agent_runs ar ON r.agent_run_id = ar.id
        """

        params = []
        if symbol:
            query += " WHERE r.symbol = ?"
            params.append(symbol)

        query += " ORDER BY r.created_at DESC LIMIT ?"
        params.append(limit)

        return pd.read_sql_query(query, self.conn, params=params)

    def get_agent_runs_summary(self, limit: int = 20) -> pd.DataFrame:
        """
        Get summary of recent agent runs.

        Args:
            limit: Number of runs to retrieve

        Returns:
            DataFrame with agent run summaries
        """
        query = """
            SELECT
                id,
                run_timestamp,
                agent_type,
                llm_provider,
                llm_model,
                assets_processed,
                assets_failed,
                execution_time_seconds,
                status,
                (SELECT COUNT(*) FROM recommendations WHERE agent_run_id = agent_runs.id) as recommendations_count
            FROM agent_runs
            ORDER BY run_timestamp DESC
            LIMIT ?
        """

        return pd.read_sql_query(query, self.conn, params=[limit])

    def close(self) -> None:
        """Close database connection."""
        if self.conn:
            self.conn.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
