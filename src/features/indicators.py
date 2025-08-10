import pandas as pd
import pandas_ta as ta

def _standardize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """Garantiza columnas: time, open, high, low, close, volume."""
    df = df.copy()

    # 🔹 Aplanar columnas si vienen como MultiIndex (Yahoo suele hacerlo)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] for col in df.columns]

    # 1) Si 'time' no está como columna, usar índice si es fecha
    if "time" not in df.columns:
        if isinstance(df.index, pd.DatetimeIndex) or df.index.name in ("Date", "Datetime"):
            df = df.reset_index()
        for cand in ("time", "Date", "Datetime", "date"):
            if cand in df.columns:
                if cand != "time":
                    df = df.rename(columns={cand: "time"})
                break

    # 2) Normalizar nombres OHLCV
    rename_map = {}
    if "Open" in df.columns:   rename_map["Open"]   = "open"
    if "High" in df.columns:   rename_map["High"]   = "high"
    if "Low" in df.columns:    rename_map["Low"]    = "low"
    if "Close" in df.columns:  rename_map["Close"]  = "close"
    elif "Adj Close" in df.columns:
        rename_map["Adj Close"] = "close"
    if "Volume" in df.columns: rename_map["Volume"] = "volume"

    if rename_map:
        df = df.rename(columns=rename_map)

    # 3) Validar columnas esenciales
    needed = ["time", "open", "high", "low", "close", "volume"]
    have = [c for c in needed if c in df.columns]
    if "time" not in have or "close" not in have:
        raise ValueError(f"Faltan columnas esenciales tras normalizar. Tengo: {df.columns.tolist()}")

    df = df[have].sort_values("time").reset_index(drop=True)
    return df


def _pick_first(d: dict, cols):
    """Devuelve la primera columna existente en d.keys() dentro de 'cols'."""
    for c in cols:
        if c in d:
            return c
    return None

def enrich_with_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = _standardize_ohlcv(df)
    out = df.copy()
    out = out.sort_values("time").reset_index(drop=True)
    # Asegura tipos
    for c in ["open","high","low","close","volume"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    # ---- Medias
    out["sma_20"]  = ta.sma(out["close"], length=20)
    out["sma_50"]  = ta.sma(out["close"], length=50)
    out["sma_200"] = ta.sma(out["close"], length=200)
    out["ema_12"]  = ta.ema(out["close"], length=12)
    out["ema_26"]  = ta.ema(out["close"], length=26)

    # ---- RSI
    rsi = ta.rsi(out["close"], length=14)
    if isinstance(rsi, pd.Series):
        out["rsi_14"] = rsi
    else:
        # a veces devuelve DataFrame
        out["rsi_14"] = rsi.iloc[:, 0]

    # ---- MACD
    macd = ta.macd(out["close"], fast=12, slow=26, signal=9)
    if macd is not None and not macd.empty:
        cols = macd.columns.tolist()
        # nombres típicos en pandas_ta
        c_macd   = _pick_first(macd, ["MACD_12_26_9","MACD_12_26_9.0"])
        c_signal = _pick_first(macd, ["MACDs_12_26_9","MACDs_12_26_9.0"])
        c_hist   = _pick_first(macd, ["MACDh_12_26_9","MACDh_12_26_9.0"])
        # fallback: coge lo que haya
        if c_macd is None and len(cols) >= 1: c_macd = cols[0]
        if c_signal is None and len(cols) >= 2: c_signal = cols[1]
        if c_hist is None and len(cols) >= 3: c_hist = cols[2]
        out["macd"]        = macd[c_macd]
        out["macd_signal"] = macd[c_signal]
        out["macd_hist"]   = macd[c_hist]

    # ---- ATR
    atr = ta.atr(out["high"], out["low"], out["close"], length=14)
    if isinstance(atr, pd.Series):
        out["atr_14"] = atr
    else:
        # algunos builds pueden devolver DataFrame con nombre distinto
        c_atr = _pick_first(atr, ["ATR_14","ATRr_14","ATRr_14.0"])
        out["atr_14"] = atr[c_atr]

    # ---- Bandas de Bollinger
    bb = ta.bbands(out["close"], length=20, std=2.0)
    if bb is not None and not bb.empty:
        # nombres habituales: BBL_20_2.0, BBM_20_2.0, BBU_20_2.0, BBB_20_2.0 (bw) o BBW_20_2.0
        c_lower = _pick_first(bb, ["BBL_20_2.0","BBL_20_2"])
        c_mid   = _pick_first(bb, ["BBM_20_2.0","BBM_20_2"])
        c_upper = _pick_first(bb, ["BBU_20_2.0","BBU_20_2"])
        c_bw    = _pick_first(bb, ["BBW_20_2.0","BBB_20_2.0","BBW_20_2","BBB_20_2"])
        out["bb_lower"] = bb[c_lower]
        out["bb_mid"]   = bb[c_mid]
        out["bb_upper"] = bb[c_upper]
        out["bb_bw"]    = bb[c_bw] if c_bw else (out["bb_upper"] - out["bb_lower"]) / out["bb_mid"]

    # ---- ADX
    adx = ta.adx(out["high"], out["low"], out["close"], length=14)
    if adx is not None and not adx.empty:
        c_adx = _pick_first(adx, ["ADX_14","ADX_14.0","ADXd_14"])  # distintas variantes
        out["adx"] = adx[c_adx]

    # ---- OBV
    obv = ta.obv(out["close"], out["volume"])
    # obv puede ser Series con nombre 'OBV'
    out["obv"] = obv if isinstance(obv, pd.Series) else obv.iloc[:, 0]

    # ---- Retornos
    out["ret_1"]  = out["close"].pct_change(1)
    out["ret_5"]  = out["close"].pct_change(5)
    out["ret_20"] = out["close"].pct_change(20)

    return out