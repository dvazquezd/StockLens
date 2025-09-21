"""Trading signal generation based on technical indicators."""

import numpy as np
import pandas as pd


class TradingSignalGenerator:
    """Generates trading signals based on technical indicators."""
    
    def generate_signals(self, df_indicators: pd.DataFrame) -> pd.DataFrame:
        """
        Generate trading signals based on technical indicators.
        
        Args:
            df_indicators: DataFrame with calculated technical indicators
            
        Returns:
            DataFrame with generated signals and recommendations
        """
        df = df_indicators.copy()
        
        # Signal generation rules
        df["signal_momentum_trend"] = np.where(
            (df["macd"] > df["macd_signal"]) & (df["adx"] > 20), 1, 0
        )
        
        df["signal_mean_reversion"] = np.where(
            df["rsi_14"] < 30, 1,
            np.where(df["rsi_14"] > 70, -1, 0)
        )
        
        df["signal_volume"] = np.where(df["obv"].diff() > 0, 1, 0)
        
        # Calculate composite score
        signal_columns = ["signal_momentum_trend", "signal_mean_reversion", "signal_volume"]
        df["score"] = df[signal_columns].sum(axis=1)
        
        # Generate recommendations
        df["recommendation"] = np.where(
            df["score"] > 0, "buy",
            np.where(df["score"] < 0, "sell", "hold")
        )
        
        return df