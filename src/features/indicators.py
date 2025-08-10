from __future__ import annotations
import pandas as pd
import warnings
warnings.filterwarnings(
    "ignore",
    message=r"pkg_resources is deprecated as an API.*",
    category=UserWarning,
    module=r"pandas_ta(\.__init__)?"
)
import pandas_ta as ta


def _standardize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """    Standardizes an OHLCV (Open, High, Low, Close, Volume) dataset to a consistent
    column format for downstream processing.

    This function:
        - Flattens MultiIndex columns if present.
        - Ensures the presence of a `time` column, converting from index or renaming
          existing date columns as needed.
        - Renames common OHLCV column variations to a standard lowercase format.
        - Validates that essential columns (`time` and `close`) are present.
        - Sorts by time and resets the index.

    Parameters:
        df (pd.DataFrame): The input DataFrame containing OHLCV market data,
            possibly with varying column names or formats.

    Returns:
        pd.DataFrame: A cleaned and standardized OHLCV DataFrame with columns:
        `time`, `open`, `high`, `low`, `close`, `volume` (where available).

    Raises:
        ValueError: If the required columns (`time` and `close`) are missing after normalization.
    """
    df = df.copy()

    # Aplanar si viniera con MultiIndex por accidente
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    # Si 'time' no está, usar índice si es fecha
    if "time" not in df.columns:
        if isinstance(df.index, pd.DatetimeIndex) or df.index.name in ("Date","Datetime"):
            df = df.reset_index()
        for cand in ("time","Date","Datetime","date"):
            if cand in df.columns:
                if cand != "time":
                    df = df.rename(columns={cand:"time"})
                break

    rename = {"Open":"open","High":"high","Low":"low","Close":"close","Adj Close":"close","Volume":"volume"}
    df = df.rename(columns=rename)

    need = ["time","open","high","low","close","volume"]
    have = [c for c in need if c in df.columns]
    if "time" not in have or "close" not in have:
        raise ValueError(f"Faltan columnas esenciales tras normalizar: {df.columns.tolist()}")

    df = df[have].sort_values("time").reset_index(drop=True)
    return df

def enrich_with_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enriches a standardized OHLCV dataset with common technical analysis indicators.

    The following indicators are calculated:
        - `rsi_14`: 14-period Relative Strength Index (RSI)
        - `macd`: MACD line (12, 26, 9)
        - `macd_signal`: MACD signal line (12, 26, 9)
        - `atr_14`: 14-period Average True Range (ATR)
        - `adx`: 14-period Average Directional Index (ADX)
        - `obv`: On-Balance Volume (OBV)

    The input DataFrame is first standardized using `_standardize_ohlcv` to ensure
    consistent column names and formats.

    Parameters:
        df (pd.DataFrame): A DataFrame containing OHLCV data with at least `time`
            and `close` columns (and ideally `open`, `high`, `low`, `volume`).

    Returns:
        pd.DataFrame: A DataFrame with `time`, `close`, and the calculated indicator columns.
            Rows containing NaN values from initial indicator lookback periods are removed.
    """
    df = _standardize_ohlcv(df)

    out = df[["time","close"]].copy()
    # Indicadores básicos (ajusta a tus funciones actuales)
    out["rsi_14"] = ta.rsi(df["close"], length=14)
    macd = ta.macd(df["close"], fast=12, slow=26, signal=9)
    if macd is not None:
        out["macd"] = macd["MACD_12_26_9"]
        out["macd_signal"] = macd["MACDs_12_26_9"]
    out["atr_14"] = ta.atr(df["high"], df["low"], df["close"], length=14)
    out["adx"] = ta.adx(df["high"], df["low"], df["close"], length=14)["ADX_14"]
    out["obv"] = ta.obv(df["close"], df["volume"])

    return out.dropna().reset_index(drop=True)
