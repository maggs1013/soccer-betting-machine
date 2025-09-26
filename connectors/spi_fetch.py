#!/usr/bin/env python3
"""
Robust FiveThirtyEight SPI fetcher (handles format drift)
- Tries fast pandas reader, then python engine with on_bad_lines='skip'
- Caches snapshot for fallback
- Output: data/sd_538_spi.csv (+ .cache.csv)
"""

import os, io, requests
import pandas as pd

DATA_DIR = "data"
OUT   = os.path.join(DATA_DIR, "sd_538_spi.csv")
CACHE = os.path.join(DATA_DIR, "sd_538_spi.cache.csv")
URL   = "https://projects.fivethirtyeight.com/soccer-api/club/spi_global_rankings2.csv"

def read_spi_df() -> pd.DataFrame:
    resp = requests.get(URL, timeout=30)
    resp.raise_for_status()
    txt = resp.text
    try:
        return pd.read_csv(io.StringIO(txt))
    except Exception:
        return pd.read_csv(io.StringIO(txt), engine="python", on_bad_lines="skip")

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    df = None
    try:
        df = read_spi_df()
    except Exception as e:
        if os.path.exists(CACHE):
            df = pd.read_csv(CACHE)
            print(f"⚠️ SPI fetch failed ({e}); using cache {len(df)} rows")
        else:
            raise SystemExit(f"SPI fetch failed and no cache available: {e}")

    # keep commonly used columns if present
    wanted = {"team","league","spi","spi_off","spi_def","rank","off","def","global_team_id","date"}
    keep = [c for c in df.columns if c.lower() in wanted]
    if keep:
        df = df[keep]

    df.to_csv(OUT, index=False)
    try:
        df.to_csv(CACHE, index=False)
    except Exception:
        pass
    print(f"✅ SPI wrote {len(df)} rows → {OUT}")

if __name__ == "__main__":
    main()