"""Local rule-based trading agent implementation."""

from pathlib import Path
from typing import List

import pandas as pd

from src.agent.agents.base import TradingAgent


class LocalAgent(TradingAgent):
    """Trading agent using local rule-based analysis (no LLM)."""

    def analyze_signals(self, processed_dir: Path) -> pd.DataFrame:
        """
        Analyze trading signals using rule-based logic.

        Args:
            processed_dir: Path to directory containing signal files

        Returns:
            DataFrame with analysis summary
        """
        print("\nExecuting Local Agent (rule-based analysis)")

        rows = []
        for file_path in sorted(processed_dir.glob("*_signals.parquet")):
            symbol = file_path.name.split("_")[0]
            df = pd.read_parquet(file_path)
            last_row = df.iloc[-1]

            rows.append({
                "symbol": symbol,
                "time": last_row["time"],
                "close": float(last_row["close"]),
                "score": int(last_row.get("score", 0)),
                "recommendation": last_row.get("recommendation", "hold"),
                "rationale": self._generate_rationale(last_row),
            })

        summary = pd.DataFrame(rows).sort_values("symbol").reset_index(drop=True)

        # Save summary to parquet
        output_file = processed_dir / "agent_summary_local.parquet"
        summary.to_parquet(output_file, index=False)
        print(f"Local Agent -> {output_file}")

        return summary

    def _generate_rationale(self, row: pd.Series) -> str:
        """
        Generate human-readable rationale for the signal.

        Args:
            row: Pandas Series representing the latest signal row

        Returns:
            Rationale string explaining the recommendation
        """
        reasons = []

        # Recommendation
        recommendation = row.get("recommendation", "hold")
        if recommendation == "buy":
            reasons.append("Buy signal")
        elif recommendation == "sell":
            reasons.append("Sell signal")
        else:
            reasons.append("Hold")

        # MACD analysis
        if "macd" in row and "macd_signal" in row:
            macd = row["macd"]
            macd_signal = row["macd_signal"]

            if macd > macd_signal:
                reasons.append("MACD > signal (bullish momentum)")
            elif macd < macd_signal:
                reasons.append("MACD < signal (bearish momentum)")

        # RSI analysis
        if "rsi_14" in row:
            rsi = row["rsi_14"]

            if rsi < 30:
                reasons.append("RSI < 30 (oversold)")
            elif rsi > 70:
                reasons.append("RSI > 70 (overbought)")

        # ADX analysis
        if "adx" in row:
            adx = row["adx"]

            if adx >= 25:
                reasons.append("Strong trend (ADX >= 25)")

        return "; ".join(reasons)
