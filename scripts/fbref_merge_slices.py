#!/usr/bin/env python3
"""
fbref_merge_slices.py — merge saved FBref slice CSVs into one table
Inputs:
  data/fbref_slice_<canonical>.csv   (written by fbref_fetch_streamlined.py)
Outputs:
  data/sd_fbref_team_stats.csv
  data/sd_fbref_team_stats.cache.csv
Notes:
- Suffix non-key columns by slice key on merge to avoid collisions
- Incremental outer merge (memory-safe)
"""

import os, glob
import pandas as pd

DATA = "data"
OUT  = os.path.join(DATA, "sd_fbref_team_stats.csv")
CACHE= os.path.join(DATA, "sd_fbref_team_stats.cache.csv")
KEYS = ("team","league","season")

def _safe_read(p):
    try: return pd.read_csv(p)
    except Exception: return pd.DataFrame()

def _suffix_nonkeys(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
    if df.empty: return df
    rename_map = {c: f"{c}_{suffix}" for c in df.columns if c not in KEYS}
    return df.rename(columns=rename_map)

def main():
    paths = sorted(glob.glob(os.path.join(DATA, "fbref_slice_*.csv")))
    if not paths:
        # fallback to existing cache if any
        if os.path.exists(CACHE):
            df = pd.read_csv(CACHE)
            df.to_csv(OUT, index=False)
            print(f"[FBref merge] no slice CSVs; used cache rows={len(df)}")
            return
        else:
            pd.DataFrame(columns=list(KEYS)).to_csv(OUT, index=False)
            print("[FBref merge] no slice CSVs; wrote stub")
            return

    merged = None
    for p in paths:
        # suffix by slice key from filename, e.g., fbref_slice_standard.csv → 'standard'
        slice_key = os.path.basename(p).replace("fbref_slice_","").replace(".csv","")
        df = _safe_read(p)
        if df.empty: 
            print(f"[FBref merge] skip empty slice {slice_key}")
            continue
        # ensure keys present
        for k in KEYS:
            if k not in df.columns:
                df[k] = None
        # suffix non-keys to avoid collisions
        df = _suffix_nonkeys(df, slice_key)
        # incremental outer merge
        merged = df if merged is None else pd.merge(merged, df, on=list(KEYS), how="outer")
        print(f"[FBref merge] merged {slice_key} → now shape={merged.shape}")

    if merged is None or merged.empty:
        if os.path.exists(CACHE):
            df = pd.read_csv(CACHE)
            df.to_csv(OUT, index=False)
            print(f"[FBref merge] all empty; used cache rows={len(df)}")
            return
        else:
            pd.DataFrame(columns=list(KEYS)).to_csv(OUT, index=False)
            print("[FBref merge] all empty; wrote stub")
            return

    # keys first
    key_cols = list(KEYS)
    other = [c for c in merged.columns if c not in key_cols]
    merged = merged[key_cols + other]

    merged.to_csv(OUT, index=False)
    try: merged.to_csv(CACHE, index=False)
    except Exception: pass
    print(f"[FBref merge] wrote {OUT} rows={len(merged)} cols={len(merged.columns)}")

if __name__ == "__main__":
    main()