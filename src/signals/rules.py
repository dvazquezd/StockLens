from __future__ import annotations
import pandas as pd
from dataclasses import dataclass

MIN_COLS = {
    "time","close","volume","ema_12","ema_26","sma_50","sma_200",
    "rsi_14","macd","macd_signal","atr_14"
}

@dataclass(frozen=True)
class SignalConfig:
    rsi_low: float = 30.0
    rsi_high: float = 70.0
    vol_ma_window: int = 20
    atr_mult: float = 2.0
    enter_threshold: int = 2  # antes era 3

def _need_cols(df: pd.DataFrame):
    miss = MIN_COLS - set(df.columns)
    if miss:
        raise ValueError(f"Faltan columnas para señales: {sorted(miss)}")

def _roll_vol_ma(df: pd.DataFrame, w: int) -> pd.Series:
    return df["volume"].rolling(w, min_periods=max(3, w//3)).mean()

def _cross_up(a: pd.Series, b: pd.Series) -> pd.Series:
    prev = (a.shift(1) <= b.shift(1))
    now  = (a > b)
    return (prev & now)

def _cross_down(a: pd.Series, b: pd.Series) -> pd.Series:
    prev = (a.shift(1) >= b.shift(1))
    now  = (a < b)
    return (prev & now)

def make_recommendations(df: pd.DataFrame, cfg: SignalConfig = SignalConfig()) -> pd.DataFrame:
    _need_cols(df)
    out = df.sort_values("time").reset_index(drop=True).copy()

    # Señales base
    trend_up   = out["sma_50"] > out["sma_200"]
    macd_up    = out["macd"] > out["macd_signal"]
    macd_xup   = _cross_up(out["macd"], out["macd_signal"])
    macd_xdn   = _cross_down(out["macd"], out["macd_signal"])
    rsi_pos    = out["rsi_14"] > 50
    rsi_oversold = out["rsi_14"] < cfg.rsi_low
    rsi_rebound  = _cross_up(out["rsi_14"], pd.Series([cfg.rsi_low]*len(out), index=out.index))

    vol_ma = _roll_vol_ma(out, cfg.vol_ma_window)
    vol_ok = out["volume"] > vol_ma

    # Momentum-trend (versión con cruce)
    s_momo = (trend_up & (macd_up | macd_xup) & rsi_pos).astype(int)

    # Mean-reversion (sobreventa + rebote + (opcional) vuelta sobre EMA12)
    price_above_ref = out["close"] > out["ema_12"]
    s_mr = (rsi_oversold & (rsi_rebound | price_above_ref)).astype(int)

    # Volumen
    s_vol = vol_ok.astype(int)

    # Score y regla de entrada
    # Entra si: (momo & volumen) o (mr & volumen) o score>=enter_threshold
    score = 2*s_momo + 1*s_mr + 1*s_vol
    enter = ( (s_momo & s_vol) | (s_mr & s_vol) | (score >= cfg.enter_threshold) )

    # Regla de salida: cruce bajista MACD o cierre < SMA50
    exit_ = (macd_xdn | (out["close"] < out["sma_50"]))

    # Gestión de estado (posición)
    position = pd.Series(False, index=out.index)
    rec = pd.Series("hold", index=out.index)

    for i in range(len(out)):
        if i == 0:
            position.iat[i] = False
        else:
            position.iat[i] = position.iat[i-1]

        if not position.iat[i] and enter.iat[i]:
            position.iat[i] = True
            rec.iat[i] = "enter"
        elif position.iat[i] and exit_.iat[i]:
            position.iat[i] = False
            rec.iat[i] = "exit"
        else:
            rec.iat[i] = "hold"

    out["sig_momentum_trend"] = s_momo
    out["sig_mean_reversion"] = s_mr
    out["sig_volume"] = s_vol
    out["score"] = score
    out["in_position"] = position
    out["recommendation"] = rec

    # Stops/targets por ATR
    out["stop_level"] = out["close"] - cfg.atr_mult*out["atr_14"]
    out["tp_level"]   = out["close"] + cfg.atr_mult*out["atr_14"]

    return out[[
        "time","close","score","recommendation","in_position",
        "stop_level","tp_level",
        "sig_momentum_trend","sig_mean_reversion","sig_volume"
    ]]
