from datetime import datetime, timezone, timedelta
import pandas as pd
from src.data_ingestion.finnhub_client import FinnhubClient, FinnhubConfig

class DummyResp:
    def __init__(self, status_code=200, json_data=None):
        self.status_code = status_code; self._json = json_data or {}
    def raise_for_status(self):
        if self.status_code >= 400: raise RuntimeError("HTTP error")
    def json(self): return self._json

def test_parse_and_save(tmp_path, monkeypatch):
    import src.data_ingestion.finnhub_client as mod
    def fake_get(url, params=None, timeout=30):
        now = int(datetime.now(timezone.utc).timestamp())
        return DummyResp(200, {"s":"ok","t":[now-60,now],"o":[1,2],"h":[2,3],"l":[0.5,1.5],"c":[1.5,2.5],"v":[10,20]})
    monkeypatch.setattr(mod.requests, "get", fake_get)

    cfg = FinnhubConfig(api_key="DUMMY", data_dir=str(tmp_path))
    client = FinnhubClient(cfg, max_req_per_min=999)
    start = int((datetime.now(timezone.utc) - timedelta(days=1)).timestamp())
    end = int(datetime.now(timezone.utc).timestamp())
    df = client.get_stock_candles("AAPL", "D", start, end, save=True)
    assert not df.empty
    assert (tmp_path / "raw" / "AAPL_D.parquet").exists()
    assert set(df.columns) == {"time","open","high","low","close","volume"}
    assert pd.api.types.is_datetime64_any_dtype(df["time"])
