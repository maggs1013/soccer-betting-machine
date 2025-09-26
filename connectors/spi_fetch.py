#!/usr/bin/env python3
"""
Robust FiveThirtyEight SPI fetcher
- Handles CSV format drift by falling back to python engine and on_bad_lines='skip'
- Caches snapshot for fallback
- Writes: data/sd_538_spi.csv
"""

import os, io, requests
import pandas as pd

DATA_DIR = "data"
OUT = os.path.join(DATA_DIR, "sd_538_spi.csv")
CACHE = os.path.join(DATA_DIR, "sd_538_spi.cache.csv")
URL = "https://projects.fivethirtyeight.com/soccer-api/club/spi_global_rankings2.csv"

def read_spi() -> pd.DataFrame:
    r = requests.get(URL, timeout=30)
    r.raise_for_status()
    text = r.text
    # 1) fast path
    try:
        return pd.read_csv(io.StringIO(text))
    except Exception:
        # 2) python engine, skip bad lines
        return pd.read_csv(io.StringIO(text), engine="python", on_bad_lines="skip")

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    df = None
    err = None
    for _ in range(2):
        try:
            df = read_spi()
            break
        except Exception as e:
            err = e

    if df is None or df.empty:
        if os.path.exists(CACHE):
            df = pd.read_csv(CACHE)
        else:
            raise SystemExit(f"SPI fetch failed and no cache available: {err}")

    # keep only columns we actually use + a few useful extras
    keep = [c for c in df.columns if c.lower() in {
        "team","league","spi","spi_off","spi_def","rank","off","def","global_team_id","date"
    }]
    if not keep:
        # fallback to all columns if names drifted
        keep = list(df.columns)
    df = df[keep]

    df.to_csv(OUT, index=False)
    try:
        df.to_csv(CACHE, index=False)
    except Exception:
        pass
    print(f"Wrote {len(df)} rows to {OUT}")

if __name__ == "__main__":
    main()