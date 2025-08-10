from __future__ import annotations
import pandas as pd
import numpy as np

def make_recommendations(df_ind: pd.DataFrame) -> pd.DataFrame:
    """
    Generates trading recommendations based on simple technical indicator rules.

    This function applies a set of predefined signal-generation rules to an
    indicator-enriched market dataset to produce momentum, mean reversion,
    and volume-based trading signals. These signals are combined into a
    composite score, which is then mapped to a textual recommendation.

    Rules:
        - Momentum trend (`sig_momentum_trend`): 1 if MACD > MACD signal and ADX > 20, else 0.
        - Mean reversion (`sig_mean_reversion`): 1 if RSI < 30, -1 if RSI > 70, else 0.
        - Volume (`sig_volume`): 1 if OBV increases from the previous value, else 0.

    The final `score` is the sum of these individual signals:
        - Positive score → "buy"
        - Negative score → "sell"
        - Zero score → "hold"

    Parameters:
        df_ind (pd.DataFrame): A pandas DataFrame containing at least the
            columns `macd`, `macd_signal`, `adx`, `rsi_14`, and `obv`.

    Returns:
        pd.DataFrame: A DataFrame including the original data plus:
            - `sig_momentum_trend`
            - `sig_mean_reversion`
            - `sig_volume`
            - `score`
            - `recommendation`
    """
    df = df_ind.copy()

    # Ejemplo de reglas simples (adaptar a las tuyas)
    df["sig_momentum_trend"] = np.where((df["macd"] > df["macd_signal"]) & (df["adx"] > 20), 1, 0)
    df["sig_mean_reversion"] = np.where(df["rsi_14"] < 30, 1, np.where(df["rsi_14"] > 70, -1, 0))
    df["sig_volume"] = np.where(df["obv"].diff() > 0, 1, 0)

    # Score y recomendación
    df["score"] = df[["sig_momentum_trend", "sig_mean_reversion", "sig_volume"]].sum(axis=1)
    df["recommendation"] = np.where(df["score"] > 0, "buy", np.where(df["score"] < 0, "sell", "hold"))

    return df
