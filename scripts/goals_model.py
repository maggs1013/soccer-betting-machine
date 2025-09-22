#!/usr/bin/env python3
import os
import pandas as pd
import numpy as np
from datetime import datetime

DATA_DIR = "data"
RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
os.makedirs(RUN_DIR, exist_ok=True)

# Canonical upstream file; keep a fallback for older runs
ENRICHED_MAIN = os.path.join(DATA_DIR, "UPCOMING_7D_enriched.csv")
ENRICHED_ALT  = os.path.join(DATA_DIR, "enriched_fixtures.csv")  # legacy fallback

OUT_MU = os.path.join(RUN_DIR, "GOALS_MU.csv")

def safe_read(path):
    if os.path.exists(path):
        try:
            return pd.read_csv(path)
        except Exception:
            pass
    return pd.DataFrame()

def main():
    # Prefer canonical file, fall back if needed
    df = safe_read(ENRICHED_MAIN)
    if df.empty:
        df = safe_read(ENRICHED_ALT)

    if df.empty:
        # Always emit a well-formed file so downstream doesn’t crash
        pd.DataFrame(columns=["fixture_id","league","mu_home","mu_away"]).to_csv(OUT_MU, index=False)
        print(f"[WARN] No enriched fixtures found; wrote empty {OUT_MU}")
        return

    # Ensure required identifiers exist
    if "fixture_id" not in df.columns:
        # build a stable ID if missing
        def _fid(r):
            d = str(r.get("date","NA")).replace("-","")
            h = str(r.get("home_team","NA")).strip().lower().replace(" ","_")
            a = str(r.get("away_team","NA")).strip().lower().replace(" ","_")
            return f"{d}__{h}__vs__{a}"
        df["fixture_id"] = df.apply(_fid, axis=1)
    if "league" not in df.columns:
        df["league"] = "GLOBAL"

    # Naive μ using rolling team xG with an SPI fallback if present
    xgh = df.get("xg_for_home_5")
    xga = df.get("xg_for_away_5")
    spi_h = df.get("spi_home", pd.Series(np.nan, index=df.index))
    spi_a = df.get("spi_away", pd.Series(np.nan, index=df.index))

    # fallback heuristics if rolling xG is missing
    mu_home = (xgh if xgh is not None else pd.Series(np.nan, index=df.index)).copy()
    mu_away = (xga if xga is not None else pd.Series(np.nan, index=df.index)).copy()

    # SPI-based fallback (light; tweak later if you prefer a different affine map)
    mu_home = mu_home.fillna(spi_h * 0.02 + 1.2)
    mu_away = mu_away.fillna(spi_a * 0.02 + 1.0)

    # If still NaN (no SPI either), plug conservative midpoints
    mu_home = mu_home.fillna(1.35)
    mu_away = mu_away.fillna(1.25)

    # Clamp to reasonable bounds
    out = df[["fixture_id","league"]].copy()
    out["mu_home"] = mu_home.clip(lower=0.2, upper=3.5)
    out["mu_away"] = mu_away.clip(lower=0.2, upper=3.5)

    out.to_csv(OUT_MU, index=False)
    print(f"[OK] GOALS_MU.csv written → {OUT_MU} rows={len(out)}")

if __name__ == "__main__":
    main()