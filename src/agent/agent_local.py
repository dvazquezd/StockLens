from pathlib import Path
import pandas as pd

def _explain(last: pd.Series) -> str:
    """
    Generates a human-readable explanation for the latest trading signal.

    This function inspects the most recent row of indicator and signal data to
    produce a rationale string. It includes reasoning based on:
        - Final recommendation (`buy`, `sell`, `hold`)
        - MACD vs. MACD signal line relationship
        - RSI overbought/oversold thresholds
        - ADX strength indication

    Parameters:
        last (pd.Series): A pandas Series representing the most recent row of
            trading signal and indicator values.

    Returns:
        str: A semicolon-separated explanation summarizing the signal decision.
    """
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
        if rsi < 30:
            reasons.append("RSI < 30 (sobreventa)")
        elif rsi > 70:
            reasons.append("RSI > 70 (sobrecompra)")

    if "adx" in last and last["adx"] >= 25:
        reasons.append("Tendencia con fuerza (ADX≥25)")

    return "; ".join(reasons)

def run_agent_local(processed_dir: Path) -> pd.DataFrame:
    """
    Runs the local analysis agent to generate a summary of the latest trading signals.

    This function:
        1. Loads the last row from each `*_signals.parquet` file in the processed directory.
        2. Extracts key fields such as `time`, `close`, `score`, and `recommendation`.
        3. Generates a human-readable rationale using `_explain`.
        4. Compiles all results into a summary DataFrame, sorted by symbol.
        5. Saves the summary to `agent_summary_local.parquet`.

    Parameters:
        processed_dir (Path): Path to the directory containing `*_signals.parquet` files.

    Returns:
        pd.DataFrame: A DataFrame summarizing the latest signals for each asset,
        with columns: `symbol`, `time`, `close`, `score`, `recommendation`, `rationale`.

    Raises:
        FileNotFoundError: If no matching `*_signals.parquet` files are found in `processed_dir`.
    """
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
