"""Trading signal generation based on technical indicators with OOP architecture."""

from __future__ import annotations
import numpy as np
import pandas as pd


class TradingSignalGenerator:
    """Generates trading signals based on technical indicators."""

    def generate_signals(self, df_indicators: pd.DataFrame) -> pd.DataFrame:
        """
        Generate trading signals based on technical indicators.

        Applies predefined signal-generation rules to produce momentum,
        mean reversion, and volume-based trading signals. These signals
        are combined into a composite score, which is mapped to a recommendation.

        Rules:
            - Momentum trend: 1 if MACD > MACD signal and ADX > 20, else 0
            - Mean reversion: 1 if RSI < 30, -1 if RSI > 70, else 0
            - Volume: 1 if OBV increases from the previous value, else 0

        The final score is the sum of these signals:
            - Positive score → "buy"
            - Negative score → "sell"
            - Zero score → "hold"

        Args:
            df_indicators: DataFrame with calculated technical indicators

        Returns:
            DataFrame with generated signals and recommendations
        """
        df = df_indicators.copy()

        # Signal generation rules
        df["sig_momentum_trend"] = np.where(
            (df["macd"] > df["macd_signal"]) & (df["adx"] > 20), 1, 0
        )

        df["sig_mean_reversion"] = np.where(
            df["rsi_14"] < 30, 1,
            np.where(df["rsi_14"] > 70, -1, 0)
        )

        df["sig_volume"] = np.where(df["obv"].diff() > 0, 1, 0)

        # Calculate composite score
        signal_columns = ["sig_momentum_trend", "sig_mean_reversion", "sig_volume"]
        df["score"] = df[signal_columns].sum(axis=1)

        # Generate recommendations
        df["recommendation"] = np.where(
            df["score"] > 0, "buy",
            np.where(df["score"] < 0, "sell", "hold")
        )

        return df


# Backward compatibility function
def make_recommendations(df_ind: pd.DataFrame) -> pd.DataFrame:
    """
    Generates trading recommendations based on technical indicator rules.
    (Backward compatibility wrapper)

    Args:
        df_ind: DataFrame containing technical indicators

    Returns:
        DataFrame including signals, score, and recommendation
    """
    generator = TradingSignalGenerator()
    return generator.generate_signals(df_ind)
