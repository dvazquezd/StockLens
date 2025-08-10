from pathlib import Path
import pandas as pd

def _explain(last: pd.Series) -> str:
    reasons = []
    if last.get("recommendation") == "buy":
        reasons.append("Señal de compra")
    elif last.get("recommendation") == "sell":
        reasons.append("Señal de venta")
    else:
        reasons.append("Mantener")

    if "macd" in last and "macd_signal" in last:
        if last["macd"] > last["macd_signal"]:
            reasons.append("MACD > señal (momento alcista)")
        elif last["macd"] < last["macd_signal"]:
            reasons.append("MACD < señal (momento bajista)")

    if "rsi_14" in last:
        rsi = last["rsi_14"]
        if rsi < 30: reasons.append("RSI < 30 (sobreventa)")
        elif rsi > 70: reasons.append("RSI > 70 (sobrecompra)")

    if "adx" in last and last["adx"] >= 25:
        reasons.append("Tendencia con fuerza (ADX≥25)")

    return "; ".join(reasons)

def run_agent_local(processed_dir: Path) -> pd.DataFrame:
    rows = []
    for f in sorted(processed_dir.glob("*_signals.parquet")):
        symbol = f.name.split("_")[0]
        df = pd.read_parquet(f)
        last = df.iloc[-1]
        rows.append({
            "symbol": symbol,
            "time":  last["time"],
            "close": float(last["close"]),
            "score": int(last.get("score", 0)),
            "recommendation": last.get("recommendation", "hold"),
            "rationale": _explain(last),
        })
    summary = pd.DataFrame(rows).sort_values("symbol").reset_index(drop=True)
    out = processed_dir / "agent_summary_local.parquet"
    summary.to_parquet(out, index=False)
    print(f"Agente local -> {out}")
    return summary
