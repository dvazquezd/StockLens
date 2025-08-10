import json
import subprocess
from pathlib import Path

CONFIG_FILE = Path("assets_config.json")

def run_command(cmd):
    """Ejecuta un comando en terminal y muestra la salida en tiempo real"""
    print(f"\n▶ Ejecutando: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print(result.stderr)

def main():
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(f"No se encontró {CONFIG_FILE}")

    with open(CONFIG_FILE, "r") as f:
        assets = json.load(f)

    for asset in assets:
        symbol = asset["symbol"]
        source = asset["source"]
        interval = asset.get("interval", "1d")

        # Descarga datos
        if source == "binance":
            limit = str(asset.get("limit", 1000))
            run_command([
                "python", "market_data.py",
                "--symbol", symbol,
                "--source", "binance",
                "--interval", interval,
                "--limit", limit
            ])
        elif source == "yahoo":
            period = asset.get("period", "1y")
            run_command([
                "python", "market_data.py",
                "--symbol", symbol,
                "--source", "yahoo",
                "--interval", interval,
                "--period", period
            ])
        else:
            print(f"⚠ Fuente desconocida para {symbol}")
            continue

        # Calcula indicadores
        run_command([
            "python", "-m", "src.features.run_indicators_example",
            "--symbol", symbol,
            "--interval", interval
        ])

        # Calcula señales
        run_command([
            "python", "-m", "src.signals.run_signals_example",
            "--symbol", symbol,
            "--interval", interval
        ])

if __name__ == "__main__":
    main()
