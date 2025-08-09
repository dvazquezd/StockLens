from __future__ import annotations
import argparse, os, time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional
import pandas as pd, requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

FINNHUB_BASE = "https://finnhub.io/api/v1"

@dataclass(frozen=True)
class FinnhubConfig:
    api_key: str
    data_dir: str = "./data"

class RateLimitError(RuntimeError): ...

class FinnhubClient:
    """Cliente mínimo para /stock/candle (OHLCV) con throttling y reintentos."""
    def __init__(self, config: FinnhubConfig, max_req_per_min: int = 50) -> None:
        self.config = config
        self.max_req_per_min = max_req_per_min
        self._window_start = time.time()
        self._count = 0
        os.makedirs(os.path.join(self.config.data_dir, "raw"), exist_ok=True)

    def _throttle(self) -> None:
        now = time.time()
        if now - self._window_start >= 60:
            self._window_start, self._count = now, 0
        if self._count >= self.max_req_per_min:
            time.sleep(max(0, 60 - (now - self._window_start)))
            self._window_start, self._count = time.time(), 0
        self._count += 1

    @retry(reraise=True, stop=stop_after_attempt(5),
           wait=wait_exponential(multiplier=1, min=1, max=30),
           retry=retry_if_exception_type((requests.RequestException, RateLimitError)))
    def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        self._throttle()
        params = {**params, "token": self.config.api_key}
        resp = requests.get(f"{FINNHUB_BASE}{path}", params=params, timeout=30)
        if resp.status_code == 429:
            raise RateLimitError("HTTP 429 Too Many Requests")
        resp.raise_for_status()
        return resp.json()

    def get_stock_candles(self, symbol: str, resolution: str,
                          start_ts: int, end_ts: int, save: bool = True) -> pd.DataFrame:
        data = self._get("/stock/candle",
                         {"symbol": symbol, "resolution": resolution, "from": start_ts, "to": end_ts})
        if data.get("s") != "ok":
            return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])
        df = pd.DataFrame({
            "time": pd.to_datetime(data["t"], unit="s", utc=True),
            "open": data["o"], "high": data["h"], "low": data["l"],
            "close": data["c"], "volume": data["v"],
        }).sort_values("time").reset_index(drop=True)
        if save:
            out = os.path.join(self.config.data_dir, "raw", f"{symbol}_{resolution}.parquet")
            df.to_parquet(out, index=False)
        return df

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Descarga OHLCV Finnhub a Parquet.")
    p.add_argument("--symbol", required=True)
    p.add_argument("--resolution", default="D")
    p.add_argument("--days", type=int, default=365)
    p.add_argument("--from_ts", type=int, default=None)
    p.add_argument("--to_ts", type=int, default=None)
    p.add_argument("--data_dir", default=None)
    return p.parse_args()

def _resolve_time_range(days: int, from_ts: Optional[int], to_ts: Optional[int]) -> tuple[int, int]:
    if from_ts and to_ts: return from_ts, to_ts
    now = datetime.now(timezone.utc); start = now - timedelta(days=days)
    return int(start.timestamp()), int(now.timestamp())

def main() -> None:
    load_dotenv()
    api_key = os.getenv("FINNHUB_API_KEY", "")
    if not api_key:
        raise RuntimeError("FINNHUB_API_KEY no configurada (.env)")
    data_dir = os.getenv("DATA_DIR", "./data")
    args = _parse_args()
    if args.data_dir: data_dir = args.data_dir
    cfg = FinnhubConfig(api_key=api_key, data_dir=data_dir)
    client = FinnhubClient(cfg)
    start_ts, end_ts = _resolve_time_range(args.days, args.from_ts, args.to_ts)
    df = client.get_stock_candles(args.symbol.upper(), args.resolution, start_ts, end_ts, save=True)
    print(f"Filas: {len(df)} → {cfg.data_dir}/raw/{args.symbol.upper()}_{args.resolution}.parquet")

if __name__ == "__main__":
    main()