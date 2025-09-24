#!/usr/bin/env python3
"""
engineer_extra_variables.py

Adds engineered variables into data/UPCOMING_7D_enriched.csv:

Windows (domestic-only):
  - PPG: last3, last5, last7, last10, season-to-date

Contrasts:
  - ppg_momentum_3_10     = last3_ppg  - last10_ppg
  - ppg_momentum_5_season = last5_ppg  - season_ppg

Fatigue / Congestion (all competitions):
  - engine_home/away_matches_3d / 7d / 10d / 14d
  - engine_home/away_days_since_last
  - engine_home/away_midweek_last7 (Tue/Wed/Thu in last 7d → 1 else 0)

Other context already handled in your other scripts:
  - engine_league_gpg_month via add_league_seasonality.py
  - finishing efficiency / travel fatigue (kept from earlier version)

UEFA detection: see UEFA_LEAGUE_TOKENS below.
"""

import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

DATA = "data"
UP   = os.path.join(DATA, "UPCOMING_7D_enriched.csv")
HIST = os.path.join(DATA, "HIST_matches.csv")

UEFA_LEAGUE_TOKENS = [
    "champions league", "uefa champions", "ucl",
    "europa league", "uel",
    "conference league", "uefa europa conference",
    "super cup"  # optional
]

def is_uefa(league_val: str) -> bool:
    if not isinstance(league_val, str): return False
    s = league_val.lower()
    return any(tok in s for tok in UEFA_LEAGUE_TOKENS)

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

def to_long_hist(H: pd.DataFrame) -> pd.DataFrame:
    H = H.copy()
    H["date"] = pd.to_datetime(H["date"], errors="coerce")
    H = H.dropna(subset=["date"]).sort_values("date")
    if "league" not in H.columns:
        H["league"] = "GLOBAL"
    h = H[["date","home_team","home_goals","away_goals","league"]].rename(
        columns={"home_team":"team","home_goals":"gf","away_goals":"ga"})
    a = H[["date","away_team","home_goals","away_goals","league"]].rename(
        columns={"away_team":"team","away_goals":"gf","home_goals":"ga"})
    long = pd.concat([h,a], ignore_index=True)
    long["pts"] = 0
    long.loc[long["gf"]>long["ga"], "pts"] = 3
    long.loc[long["gf"]==long["ga"], "pts"] = 1
    long["is_uefa"] = long["league"].astype(str).apply(is_uefa).astype(int)
    return long.sort_values(["team","date"])

def rolling_ppg(long: pd.DataFrame, team: str, n: int, domestic_only: bool) -> float:
    g = long[long["team"]==team]
    if domestic_only:
        g = g[g["is_uefa"]==0]
    if g.empty: return np.nan
    g = g.sort_values("date")
    return g["pts"].rolling(n, min_periods=1).mean().iloc[-1]

def season_ppg(long: pd.DataFrame, team: str, date_ref: pd.Timestamp, domestic_only: bool) -> float:
    g = long[long["team"]==team]
    if domestic_only:
        g = g[g["is_uefa"]==0]
    if g.empty: return np.nan
    season = date_ref.year
    g_season = g[g["date"].dt.year==season]
    return g_season["pts"].mean() if not g_season.empty else np.nan

def congestion_counts(long_all: pd.DataFrame, team: str, date_ref: pd.Timestamp, days: int) -> int:
    start = date_ref - timedelta(days=days)
    g = long_all[(long_all["team"]==team) & (long_all["date"] < date_ref) & (long_all["date"] >= start)]
    return int(len(g))

def days_since_last(long_all: pd.DataFrame, team: str, date_ref: pd.Timestamp) -> float:
    g = long_all[(long_all["team"]==team) & (long_all["date"] < date_ref)]
    if g.empty: return np.nan
    return float((date_ref - g["date"].iloc[-1]).days)

def midweek_last7(long_all: pd.DataFrame, team: str, date_ref: pd.Timestamp) -> int:
    start = date_ref - timedelta(days=7)
    g = long_all[(long_all["team"]==team) & (long_all["date"] < date_ref) & (long_all["date"] >= start)]
    if g.empty: return 0
    # Tue(1)/Wed(2)/Thu(3)
    return int(any(d in (1,2,3) for d in g["date"].dt.weekday))

def main():
    up = safe_read(UP)
    if up.empty:
        print(f"[WARN] {UP} missing/empty; nothing to engineer.")
        return
    if "league" not in up.columns: up["league"] = "GLOBAL"
    up["date"] = pd.to_datetime(up.get("date", pd.NaT), errors="coerce")

    H = safe_read(HIST, ["date","home_team","away_team","home_goals","away_goals","league"])
    if H.empty:
        # emit empty engineered columns
        cols = [
            "engine_home_last3_ppg","engine_away_last3_ppg",
            "engine_home_last5_ppg","engine_away_last5_ppg",
            "engine_home_last7_ppg","engine_away_last7_ppg",
            "engine_home_last10_ppg","engine_away_last10_ppg",
            "engine_home_season_ppg","engine_away_season_ppg",
            "engine_home_ppg_momentum_3_10","engine_away_ppg_momentum_3_10",
            "engine_home_ppg_momentum_5_season","engine_away_ppg_momentum_5_season",
            "engine_home_matches_3d","engine_away_matches_3d",
            "engine_home_matches_7d","engine_away_matches_7d",
            "engine_home_matches_10d","engine_away_matches_10d",
            "engine_home_matches_14d","engine_away_matches_14d",
            "engine_home_days_since_last","engine_away_days_since_last",
            "engine_home_midweek_last7","engine_away_midweek_last7"
        ]
        for c in cols: up[c] = np.nan
        up.to_csv(UP, index=False)
        print(f"[OK] engineered variables (empty HIST) → {UP}")
        return

    long = to_long_hist(H)             # all competitions
    long_dom = long[long["is_uefa"]==0]  # domestic only

    # compute windows/domestic + congestion/all comps per fixture
    ecols = {}
    for r in up.itertuples(index=False):
        dt = getattr(r, "date")
        ht = getattr(r, "home_team")
        at = getattr(r, "away_team")
        # windows: domestic-only, season anchored to fixture year
        h3  = rolling_ppg(long_dom, ht, 3,  domestic_only=True)
        h5  = rolling_ppg(long_dom, ht, 5,  domestic_only=True)
        h7  = rolling_ppg(long_dom, ht, 7,  domestic_only=True)
        h10 = rolling_ppg(long_dom, ht, 10, domestic_only=True)
        hs  = season_ppg(long_dom, ht, dt,  domestic_only=True)

        a3  = rolling_ppg(long_dom, at, 3,  domestic_only=True)
        a5  = rolling_ppg(long_dom, at, 5,  domestic_only=True)
        a7  = rolling_ppg(long_dom, at, 7,  domestic_only=True)
        a10 = rolling_ppg(long_dom, at, 10, domestic_only=True)
        as_ = season_ppg(long_dom, at, dt,  domestic_only=True)

        # momentum
        h_m3_10 = (h3 - h10) if np.isfinite(h3) and np.isfinite(h10) else np.nan
        h_m5_s  = (h5 - hs)  if np.isfinite(h5) and np.isfinite(hs)  else np.nan
        a_m3_10 = (a3 - a10) if np.isfinite(a3) and np.isfinite(a10) else np.nan
        a_m5_s  = (a5 - as_) if np.isfinite(a5) and np.isfinite(as_) else np.nan

        # congestion (all competitions)
        h3d  = congestion_counts(long, ht, dt, 3)
        h7d  = congestion_counts(long, ht, dt, 7)
        h10d = congestion_counts(long, ht, dt, 10)
        h14d = congestion_counts(long, ht, dt, 14)
        a3d  = congestion_counts(long, at, dt, 3)
        a7d  = congestion_counts(long, at, dt, 7)
        a10d = congestion_counts(long, at, dt, 10)
        a14d = congestion_counts(long, at, dt, 14)

        hds  = days_since_last(long, ht, dt)
        ads  = days_since_last(long, at, dt)
        hmw7 = midweek_last7(long, ht, dt)
        amw7 = midweek_last7(long, at, dt)

        ecols.setdefault("engine_home_last3_ppg", []).append(h3)
        ecols.setdefault("engine_home_last5_ppg", []).append(h5)
        ecols.setdefault("engine_home_last7_ppg", []).append(h7)
        ecols.setdefault("engine_home_last10_ppg", []).append(h10)
        ecols.setdefault("engine_home_season_ppg", []).append(hs)
        ecols.setdefault("engine_home_ppg_momentum_3_10", []).append(h_m3_10)
        ecols.setdefault("engine_home_ppg_momentum_5_season", []).append(h_m5_s)

        ecols.setdefault("engine_away_last3_ppg", []).append(a3)
        ecols.setdefault("engine_away_last5_ppg", []).append(a5)
        ecols.setdefault("engine_away_last7_ppg", []).append(a7)
        ecols.setdefault("engine_away_last10_ppg", []).append(a10)
        ecols.setdefault("engine_away_season_ppg", []).append(as_)
        ecols.setdefault("engine_away_ppg_momentum_3_10", []).append(a_m3_10)
        ecols.setdefault("engine_away_ppg_momentum_5_season", []).append(a_m5_s)

        ecols.setdefault("engine_home_matches_3d", []).append(h3d)
        ecols.setdefault("engine_home_matches_7d", []).append(h7d)
        ecols.setdefault("engine_home_matches_10d", []).append(h10d)
        ecols.setdefault("engine_home_matches_14d", []).append(h14d)
        ecols.setdefault("engine_away_matches_3d", []).append(a3d)
        ecols.setdefault("engine_away_matches_7d", []).append(a7d)
        ecols.setdefault("engine_away_matches_10d", []).append(a10d)
        ecols.setdefault("engine_away_matches_14d", []).append(a14d)
        ecols.setdefault("engine_home_days_since_last", []).append(hds)
        ecols.setdefault("engine_away_days_since_last", []).append(ads)
        ecols.setdefault("engine_home_midweek_last7", []).append(hmw7)
        ecols.setdefault("engine_away_midweek_last7", []).append(amw7)

    for k, v in ecols.items():
        up[k] = v

    up.to_csv(UP, index=False)
    print(f"[OK] engineered variables merged → {UP}")

if __name__ == "__main__":
    main()