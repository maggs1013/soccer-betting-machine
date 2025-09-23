#!/usr/bin/env python3
"""
add_league_seasonality.py
Compute league seasonality effects (goals per game by month) from HIST and
merge into UPCOMING_7D_enriched.csv.

Outputs:
  - data/LEAGUE_SEASONALITY.csv (league, month, engine_league_gpg_month)
  - data/UPCOMING_7D_enriched.csv (adds engine_league_gpg_month for each fixture)
"""

import os
import numpy as np
import pandas as pd

DATA = "data"
HIST = os.path.join(DATA, "HIST_matches.csv")
UP   = os.path.join(DATA, "UPCOMING_7D_enriched.csv")
OUTS = os.path.join(DATA, "LEAGUE_SEASONALITY.csv")

def safe_read(path, cols=None):
    if not os.path.exists(path):
        return pd.DataFrame(columns=cols or [])
    try:
        df = pd.read_csv(path)
    except Exception:
        return pd.DataFrame(columns=cols or [])
    if cols:
        for c in cols:
            if c not in df.columns: df[c] = np.nan
    return df

def main():
    up = safe_read(UP)
    if up.empty:
        print(f"[WARN] {UP} missing/empty; wrote no seasonality.")
        # still emit an empty seasonality file
        pd.DataFrame(columns=["league","month","engine_league_gpg_month"]).to_csv(OUTS, index=False)
        return

    hist = safe_read(HIST, ["date","home_team","away_team","home_goals","away_goals","league"])
    if hist.empty:
        # fallback: global neutral 2.60
        if "league" not in up.columns: up["league"] = "GLOBAL"
        if "date" not in up.columns:
            up["engine_league_gpg_month"] = 2.60
        else:
            up["engine_league_gpg_month"] = 2.60
        up.to_csv(UP, index=False)
        pd.DataFrame(columns=["league","month","engine_league_gpg_month"]).to_csv(OUTS, index=False)
        print(f"[OK] seasonality fallback merged → {UP}")
        return

    # compute month-level league gpg
    hist["date"] = pd.to_datetime(hist["date"], errors="coerce")
    hist = hist.dropna(subset=["date"]).sort_values("date")
    if "league" not in hist.columns: hist["league"] = "GLOBAL"
    hist["month"] = hist["date"].dt.month
    hist["gpg"] = (hist["home_goals"] + hist["away_goals"]).astype(float)
    seasonality = hist.groupby(["league","month"], as_index=False)["gpg"].mean().rename(
        columns={"gpg":"engine_league_gpg_month"})

    # save table
    seasonality.to_csv(OUTS, index=False)

    # merge into upcoming by league + month of fixture date
    up["date"] = pd.to_datetime(up.get("date", pd.NaT), errors="coerce")
    up["month"] = up["date"].dt.month
    if "league" not in up.columns: up["league"] = "GLOBAL"
    up = up.merge(seasonality, on=["league","month"], how="left")
    up.drop(columns=["month"], inplace=True)
    # fill with league median or global default
    if "engine_league_gpg_month" in up.columns:
        med = up["engine_league_gpg_month"].median()
        up["engine_league_gpg_month"] = up["engine_league_gpg_month"].fillna(med if np.isfinite(med) else 2.60)
    else:
        up["engine_league_gpg_month"] = 2.60

    up.to_csv(UP, index=False)
    print(f"[OK] seasonality merged → {UP} and wrote {OUTS}")

if __name__ == "__main__":
    main()