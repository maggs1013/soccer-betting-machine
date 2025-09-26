import argparse, pandas as pd
from utils import http_get, write_json, horizon_dates

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--key", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    start, end = horizon_dates(7)
    base = "https://api.the-odds-api.com/v4/sports"
    sports = http_get(f"{base}/", params={"apiKey": args.key})
    meta = {"ok": False, "ms": None, "sample": None, "capabilities": {}}

    if hasattr(sports, "status_code"):
        meta["ok"] = sports.status_code == 200
        meta["ms"] = sports.elapsed.total_seconds() * 1000 if meta["ok"] else None
        if meta["ok"]:
            ls = sports.json()
            soccer = [s for s in ls if "soccer_" in s["key"]]
            meta["sample"] = soccer[:10]
            # Probe one league for odds types & markets (if allowed under plan)
            caps = {"markets": ["h2h","totals","btts","spreads"], "has_outrights": True}
            meta["capabilities"]["generic"] = caps

    # Probe future odds for next 7 days (pseudo: dependent on plan/limits)
    # In practice you would loop leagues and count events per horizon bucket
    horizon = {"24h": 0, "72h": 0, "7d": 0}
    meta["future_odds_counts"] = horizon

    write_json(meta, args.out)

if __name__ == "__main__":
    main()