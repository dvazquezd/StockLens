from __future__ import annotations
import pandas as pd
import pandas_ta as ta

REQUIRED_COLS = {"time", "open", "high", "low", "close", "volume"}

def enrich_with_indicators(
    df: pd.DataFrame,
    *,
    rsi_len: int = 14,
    macd_fast: int = 12,
    macd_slow: int = 26,
    macd_signal: int = 9,
    atr_len: int = 14,
    bb_len: int = 20,
    bb_std: float = 2.0,
    adx_len: int = 14,
    sma_list: tuple[int, ...] = (20, 50, 200),
    ema_list: tuple[int, ...] = (12, 26),
) -> pd.DataFrame:
    """
    Recibe un DataFrame con columnas ['time','open','high','low','close','volume']
    y devuelve el mismo DataFrame con indicadores técnicos añadidos.
    """
    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas requeridas: {missing}")

    out = df.copy()
    out = out.sort_values("time").reset_index(drop=True)
    out = out.drop_duplicates(subset=["time"])
    out["time"] = pd.to_datetime(out["time"], utc=True)
    out = out.set_index("time")

    # Tendencia
    for n in sma_list:
        out[f"sma_{n}"] = ta.sma(out["close"], length=n)
    for n in ema_list:
        out[f"ema_{n}"] = ta.ema(out["close"], length=n)

    # Momentum
    out[f"rsi_{rsi_len}"] = ta.rsi(out["close"], length=rsi_len)
    macd_df = ta.macd(out["close"], fast=macd_fast, slow=macd_slow, signal=macd_signal)
    if macd_df is not None:
        out["macd"] = macd_df.iloc[:, 0]
        out["macd_signal"] = macd_df.iloc[:, 1]
        out["macd_hist"] = macd_df.iloc[:, 2]

    # Volatilidad
    out[f"atr_{atr_len}"] = ta.atr(out["high"], out["low"], out["close"], length=atr_len)
    bb = ta.bbands(out["close"], length=bb_len, std=bb_std)
    if bb is not None:
        out["bb_lower"] = bb.iloc[:, 0]
        out["bb_mid"]   = bb.iloc[:, 1]
        out["bb_upper"] = bb.iloc[:, 2]
        out["bb_bw"]    = (out["bb_upper"] - out["bb_lower"]) / out["bb_mid"]

    # Fuerza de tendencia
    adx_df = ta.adx(out["high"], out["low"], out["close"], length=adx_len)
    if adx_df is not None:
        out["adx"] = adx_df.iloc[:, 0]  # ADX_14

    # Volumen / confirmación
    out["obv"] = ta.obv(out["close"], out["volume"])

    # Retornos auxiliares
    out["ret_1"] = out["close"].pct_change()
    out["ret_5"] = out["close"].pct_change(5)
    out["ret_20"] = out["close"].pct_change(20)

    return out.reset_index()
