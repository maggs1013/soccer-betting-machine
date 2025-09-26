#!/usr/bin/env python3
"""
spi_fetch.py — robust FiveThirtyEight parser + ci_width
Writes: data/sd_538_spi.csv (+ cache)
"""

import os, io, requests
import pandas as pd

DATA_DIR = "data"
OUT   = os.path.join(DATA_DIR, "sd_538_spi.csv")
CACHE = os.path.join(DATA_DIR, "sd_538_spi.cache.csv")
URL   = "https://projects.fivethirtyeight.com/soccer-api/club/spi_global_rankings2.csv"

def read_spi_df() -> pd.DataFrame:
    r = requests.get(URL, timeout=30); r.raise_for_status()
    txt = r.text
    try:
        df = pd.read_csv(io.StringIO(txt))
    except Exception:
        df = pd.read_csv(io.StringIO(txt), engine="python", on_bad_lines="skip")
    return df

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    df = None
    try:
        df = read_spi_df()
    except Exception as e:
        if os.path.exists(CACHE):
            df = pd.read_csv(CACHE)
            print(f"⚠️ SPI fetch failed ({e}); using cache")
        else:
            raise SystemExit(f"SPI fetch failed and no cache available: {e}")

    # Keep common + CI if present
    keep = [c for c in df.columns if c.lower() in {
        "team","league","spi","spi_off","spi_def","rank","off","def","global_team_id","date",
        "spi_low","spi_high","spi_ci_low","spi_ci_high","spi_conf_int_low","spi_conf_int_high"
    }]
    if keep:
        df = df[keep]

    # Compute ci_width if possible
    def pick(*names):
        for n in names:
            if n in df.columns: return n
        return None
    low = pick("spi_conf_int_low","spi_ci_low","spi_low")
    high = pick("spi_conf_int_high","spi_ci_high","spi_high")
    if low and high:
        try:
            df["spi_ci_width"] = df[high].astype(float) - df[low].astype(float)
        except Exception:
            df["spi_ci_width"] = pd.NA
    else:
        df["spi_ci_width"] = pd.NA

    df.to_csv(OUT, index=False)
    try: df.to_csv(CACHE, index=False)
    except Exception: pass
    print(f"✅ SPI wrote {len(df)} rows → {OUT}")

if __name__ == "__main__":
    main()