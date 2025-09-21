"""Local rule-based trading analysis agent."""

from pathlib import Path
from typing import List

import pandas as pd


class LocalTradingAgent:
    """Local rule-based trading analysis agent."""
    
    def _generate_explanation(self, latest_signal: pd.Series) -> str:
        """
        Generate human-readable explanation for trading signal.
        
        Args:
            latest_signal: Latest signal data as pandas Series
            
        Returns:
            Semicolon-separated explanation string
        """
        explanations = []
        
        # Basic recommendation
        recommendation = latest_signal.get("recommendation", "hold")
        if recommendation == "buy":
            explanations.append("Buy signal")
        elif recommendation == "sell":
            explanations.append("Sell signal")
        else:
            explanations.append("Hold position")
        
        # MACD analysis
        macd = latest_signal.get("macd")
        macd_signal = latest_signal.get("macd_signal")
        if macd is not None and macd_signal is not None:
            if macd > macd_signal:
                explanations.append("MACD > signal (bullish momentum)")
            elif macd < macd_signal:
                explanations.append("MACD < signal (bearish momentum)")
        
        # RSI analysis
        rsi = latest_signal.get("rsi_14")
        if rsi is not None:
            if rsi < 30:
                explanations.append("RSI < 30 (oversold)")
            elif rsi > 70:
                explanations.append("RSI > 70 (overbought)")
        
        # ADX trend strength
        adx = latest_signal.get("adx")
        if adx is not None and adx >= 25:
            explanations.append("Strong trend (ADX≥25)")
        
        return "; ".join(explanations)
    
    def analyze_signals(self, processed_directory: Path) -> pd.DataFrame:
        """
        Analyze trading signals using local rules and generate summary.
        
        Args:
            processed_directory: Directory containing processed signal files
            
        Returns:
            DataFrame with analysis summary
        """
        signal_files = list(processed_directory.glob("*_signals.parquet"))
        if not signal_files:
            raise FileNotFoundError(f"No signal files found in {processed_directory}")
        
        analysis_results = []
        
        for file_path in sorted(signal_files):
            symbol = file_path.name.split("_")[0]
            df = pd.read_parquet(file_path)
            latest_signal = df.iloc[-1]
            
            analysis_results.append({
                "symbol": symbol,
                "time": latest_signal["time"],
                "close": float(latest_signal["close"]),
                "score": int(latest_signal.get("score", 0)),
                "recommendation": latest_signal.get("recommendation", "hold"),
                "rationale": self._generate_explanation(latest_signal),
            })
        
        summary_df = pd.DataFrame(analysis_results).sort_values("symbol").reset_index(drop=True)
        
        # Save results
        output_file = processed_directory / "agent_summary_local.parquet"
        summary_df.to_parquet(output_file, index=False)
        print(f"Local agent -> {output_file}")
        
        return summary_df