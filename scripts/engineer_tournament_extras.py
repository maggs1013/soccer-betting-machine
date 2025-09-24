#!/usr/bin/env python3
"""
engineer_tournament_extras.py

Goal: counter sparse tournament data by injecting credible priors derived from
existing, richer sources (SPI + domestic HIST). Merges into UPCOMING_7D_enriched.csv.

Adds (per fixture):
  - engine_home_league_strength, engine_away_league_strength
  - engine_home_team_spi,        engine_away_team_spi
  - engine_lsi_diff = H_LSI - A_LSI
  - engine_tsi_diff = H_TSI - A_TSI
  - engine_home_euro_matches_365/730, engine_away_euro_matches_365/730
  - engine_home_euro_wr_shrunk,       engine_away_euro_wr_shrunk

Inputs:
  data/UPCOMING_7D_enriched.csv  (must contain date, league, home_team, away_team)
  data/sd_538_spi.csv            (team-level SPI; we infer columns)
  data/HIST_matches.csv          (historical results with league)

Outputs:
  Overwrites data/UPCOMING_7D_enriched.csv with new columns merged in.

Notes:
  - SPI parsing: we look for any two numeric columns labeled like "off"/"def" (case-insensitive).
  - League strength prior (LSI): league mean of (spi_off - spi_def).
  - Team strength prior (TSI): team mean of  (spi_off - spi_def).
  - UEFA detection uses tokens in league string (no web calls).
"""

import os
import numpy as np
import pandas as pd

DATA = "data"
UP   = os.path.join(DATA, "UPCOMING_7D_enriched.csv")
SPI  = os.path.join(DATA, "sd_538_spi.csv")
HIST = os.path.join(DATA, "HIST_matches.csv")

UEFA_TOKENS = [
    "champions league","uefa champions","ucl",
    "europa league","uefa europa","uel",
    "conference league","uefa europa conference","uecl","super cup"
]

def is_uefa(league):
    if not isinstance(league, str): return False
    s = league.lower()
    return any(tok in s for tok in UEFA_TOKENS)

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

def parse_spi(spi_df: pd.DataFrame):
    """Return (team_spi_table, league_spi_table) with columns:
       team_spi_table: team, tsi
       league_spi_table: league, lsi
    """
    if spi_df.empty:
        return (pd.DataFrame(columns=["team","tsi"]), pd.DataFrame(columns=["league","lsi"]))
    df = spi_df.copy()
    # Standardize team col
    if "team" not in df.columns:
        for c in ["squad","team_name","name"]:
            if c in df.columns:
                df = df.rename(columns={c:"team"})
                break
    # Find off/def columns (case-insensitive)
    lc = {c.lower(): c for c in df.columns}
    off_col = lc.get("off") or lc.get("offense") or lc.get("spi_off") or None
    def_col = lc.get("def") or lc.get("defense") or lc.get("spi_def") or None

    if ("team" not in df.columns) or (off_col is None) or (def_col is None):
        return (pd.DataFrame(columns=["team","tsi"]), pd.DataFrame(columns=["league","lsi"]))

    # TSI per team (mean over table)
    df["_tsi"] = pd.to_numeric(df[off_col], errors="coerce") - pd.to_numeric(df[def_col], errors="coerce")
    team_spi = df.groupby("team", as_index=False)["_tsi"].mean().rename(columns={"_tsi":"tsi"})

    # If there's a league column in SPI table, compute per-league LSI; else return empty
    if "league" in df.columns:
        league_spi = df.groupby("league", as_index=False)["_tsi"].mean().rename(columns={"_tsi":"lsi"})
    else:
        league_spi = pd.DataFrame(columns=["league","lsi"])

    return (team_spi, league_spi)

def build_uefa_experience(hist_df: pd.DataFrame):
    """Return per-team UEFA experience summary:
       team, euro_matches_365, euro_matches_730, euro_wr_shrunk
       win-rate shrunk to prior 1/3 with alpha=5 (i.e., prior weight ~5 games)
    """
    if hist_df.empty or "league" not in hist_df.columns:
        return pd.DataFrame(columns=[
            "team","euro_matches_365","euro_matches_730","euro_wr_shrunk"
        ])

    df = hist_df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    # UEFA only
    df = df[df["league"].astype(str).apply(is_uefa)]
    if df.empty:
        return pd.DataFrame(columns=["team","euro_matches_365","euro_matches_730","euro_wr_shrunk"])

    # Long format with result flag for 1X2 home/draw/away (home team perspective for win)
    # We’ll count team "win" as (goals_for > goals_against) in its row.
    h = df[["date","home_team","home_goals","away_goals"]].rename(
        columns={"home_team":"team","home_goals":"gf","away_goals":"ga"})
    a = df[["date","away_team","home_goals","away_goals"]].rename(
        columns={"away_team":"team","away_goals":"gf","home_goals":"ga"})
    long = pd.concat([h,a], ignore_index=True)
    long["win"] = (long["gf"] > long["ga"]).astype(int)

    max_date = long["date"].max()
    cutoff_365 = max_date - pd.Timedelta(days=365)
    cutoff_730 = max_date - pd.Timedelta(days=730)

    def agg(team_df):
        m365 = int((team_df["date"] >= cutoff_365).sum())
        m730 = int((team_df["date"] >= cutoff_730).sum())
        # use all available matches for WR; shrink to prior p0=1/3 with alpha=5
        n = len(team_df)
        wr = float(team_df["win"].mean()) if n > 0 else np.nan
        p0, alpha = 1.0/3.0, 5.0
        wr_shrunk = (wr*n + p0*alpha) / (n + alpha) if n > 0 else p0
        return pd.Series({"euro_matches_365":m365, "euro_matches_730":m730, "euro_wr_shrunk":wr_shrunk})

    exp = long.groupby("team", as_index=False).apply(agg)
    if isinstance(exp.index, pd.MultiIndex):
        exp = exp.reset_index(level=0, drop=True).reset_index().rename(columns={"index":"team"})
    return exp

def main():
    up = safe_read(UP)
    if up.empty:
        print(f"[WARN] {UP} missing/empty; nothing to engineer."); return
    if "league" not in up.columns: up["league"] = "GLOBAL"

    spi_df = safe_read(SPI)
    hist = safe_read(HIST, ["date","home_team","away_team","home_goals","away_goals","league"])

    # --- SPI-derived priors ---
    team_spi, league_spi = parse_spi(spi_df)

    # If league_spi is empty, try to estimate from HIST by averaging team TSI
    if league_spi.empty and not team_spi.empty and "league" in spi_df.columns:
        league_spi = spi_df.groupby("league", as_index=False)[["_tsi"]].mean().rename(columns={"_tsi":"lsi"})

    # Fallback: if still empty, approximate via HIST domestic xG diff (low weight proxy)
    if league_spi.empty and not hist.empty:
        # use team-level domestic GD per match as proxy, then league mean
        hist["date"] = pd.to_datetime(hist["date"], errors="coerce")
        hist = hist.dropna(subset=["date"])
        if "league" not in hist.columns: hist["league"] = "GLOBAL"
        h = hist[["league","home_team","home_goals","away_goals"]].rename(
            columns={"home_team":"team","home_goals":"gf","away_goals":"ga"})
        a = hist[["league","away_team","home_goals","away_goals"]].rename(
            columns={"away_team":"team","away_goals":"gf","home_goals":"ga"})
        long = pd.concat([h,a], ignore_index=True)
        long["gd"] = pd.to_numeric(long["gf"], errors="coerce") - pd.to_numeric(long["ga"], errors="coerce")
        league_spi = long.groupby("league", as_index=False)["gd"].mean().rename(columns={"gd":"lsi"})

    # Thin team_spi? keep empty but safe
    team_spi = team_spi if not team_spi.empty else pd.DataFrame(columns=["team","tsi"])

    # --- UEFA experience summary ---
    exp = build_uefa_experience(hist)

    # --- Merge into UPCOMING ---
    df = up.copy()

    # Join league strength
    if not league_spi.empty:
        lspi = league_spi.rename(columns={"lsi":"engine_league_strength"})
        df = df.merge(lspi, on="league", how="left")

    # Join team SPI as TSI proxy
    if not team_spi.empty:
        hspi = team_spi.rename(columns={"team":"home_team","tsi":"engine_home_team_spi"})
        aspi = team_spi.rename(columns={"team":"away_team","tsi":"engine_away_team_spi"})
        df = df.merge(hspi, on="home_team", how="left").merge(aspi, on="away_team", how="left")

    # Diffs
    df["engine_lsi_diff"] = df.get("engine_league_strength", np.nan)
    # If we have both leagues (same column), lsi_diff is 0; else compute via per-row merge approach:
    # Since we only merged a single league-level column, approximate by re-merging away league strength:
    if "engine_league_strength" in df.columns:
        # Create away league strength via a second merge
        away_lsi = league_spi.rename(columns={"league":"_away_league","lsi":"_away_lsi"}) if not league_spi.empty else pd.DataFrame()
        if not away_lsi.empty and "_away_league" not in df.columns:
            df["_away_league"] = df["league"]  # leagues are same col; many fixtures are intra-league; keep as-is
        if not away_lsi.empty:
            df = df.merge(away_lsi, left_on="_away_league", right_on="_away_league", how="left")
            df["engine_lsi_diff"] = df["engine_league_strength"] - df["_away_lsi"]
        df.drop(columns=[c for c in ["_away_league","_away_lsi"] if c in df.columns], inplace=True)

    df["engine_tsi_diff"] = df.get("engine_home_team_spi", np.nan) - df.get("engine_away_team_spi", np.nan)

    # UEFA experience onto home/away
    if not exp.empty:
        hexp = exp.rename(columns={
            "team":"home_team",
            "euro_matches_365":"engine_home_euro_matches_365",
            "euro_matches_730":"engine_home_euro_matches_730",
            "euro_wr_shrunk":"engine_home_euro_wr_shrunk"
        })
        aexp = exp.rename(columns={
            "team":"away_team",
            "euro_matches_365":"engine_away_euro_matches_365",
            "euro_matches_730":"engine_away_euro_matches_730",
            "euro_wr_shrunk":"engine_away_euro_wr_shrunk"
        })
        df = df.merge(hexp, on="home_team", how="left").merge(aexp, on="away_team", how="left")

    # Write
    df.to_csv(UP, index=False)
    print(f"[OK] engineered tournament extras merged → {UP} (rows={len(df)})")

if __name__ == "__main__":
    main()