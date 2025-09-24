#!/usr/bin/env python3
"""
engineer_tournament_extras.py

Counter sparse tournament data by injecting priors derived from your existing sources:
  - League Strength (LSI): mean (spi_off - spi_def) per DOMESTIC league
  - Team Strength (TSI):  mean (spi_off - spi_def) per team
  - Diffs: engine_lsi_diff (home_dom - away_dom), engine_tsi_diff (home - away)
  - UEFA experience: matches last 365/730 days, shrunk win-rate (prior 1/3, alpha=5)

Writes into: data/UPCOMING_7D_enriched.csv
Inputs: data/UPCOMING_7D_enriched.csv, data/sd_538_spi.csv, data/HIST_matches.csv
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
    if not os.path.exists(path): return pd.DataFrame(columns=cols or [])
    try:
        df = pd.read_csv(path)
    except Exception:
        return pd.DataFrame(columns=cols or [])
    if cols:
        for c in cols:
            if c not in df.columns: df[c] = np.nan
    return df

def parse_spi(spi_df: pd.DataFrame):
    """Return (team_spi, league_spi) with:
       team_spi:   team, tsi
       league_spi: league, lsi
    """
    if spi_df.empty:
        return (pd.DataFrame(columns=["team","tsi"]),
                pd.DataFrame(columns=["league","lsi"]))

    df = spi_df.copy()

    # Standardize team column
    if "team" not in df.columns:
        for alt in ["squad","team_name","name"]:
            if alt in df.columns:
                df = df.rename(columns={alt:"team"})
                break

    lc = {c.lower(): c for c in df.columns}
    off_col = lc.get("off") or lc.get("offense") or lc.get("spi_off")
    def_col = lc.get("def") or lc.get("defense") or lc.get("spi_def")

    if "team" not in df.columns or off_col is None or def_col is None:
        return (pd.DataFrame(columns=["team","tsi"]),
                pd.DataFrame(columns=["league","lsi"]))

    df["_tsi"] = pd.to_numeric(df[off_col], errors="coerce") - pd.to_numeric(df[def_col], errors="coerce")

    # TSI per team
    team_spi  = df.groupby("team", as_index=False)["_tsi"].mean().rename(columns={"_tsi":"tsi"})

    # LSI per league if SPI supplies a 'league' column
    if "league" in df.columns:
        league_spi = df.groupby("league", as_index=False)["_tsi"].mean().rename(columns={"_tsi":"lsi"})
    else:
        league_spi = pd.DataFrame(columns=["league","lsi"])

    return team_spi, league_spi

def build_domestic_mapping(hist_df: pd.DataFrame):
    """
    Map each team -> most-recent domestic league using HIST (exclude UEFA).
    Returns DataFrame: team, dom_league
    """
    if hist_df.empty:
        return pd.DataFrame(columns=["team","dom_league"])

    df = hist_df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    if "league" not in df.columns: df["league"] = "GLOBAL"

    # Exclude UEFA competitions -> domestic only
    df = df[~df["league"].astype(str).apply(is_uefa)]
    if df.empty:
        return pd.DataFrame(columns=["team","dom_league"])

    # Build long per-team rows with league label
    h = df[["date","home_team","league"]].rename(columns={"home_team":"team"})
    a = df[["date","away_team","league"]].rename(columns={"away_team":"team"})
    long = pd.concat([h,a], ignore_index=True).sort_values("date")

    # For each team choose most recent league appearance (or mode)
    # We'll take the last seen league value (most recent) per team.
    idx = long.groupby("team")["date"].idxmax()
    recent = long.loc[idx, ["team","league"]].rename(columns={"league":"dom_league"}).reset_index(drop=True)
    return recent

def build_uefa_experience(hist_df: pd.DataFrame):
    """Return per-team UEFA experience:
       team, euro_matches_365, euro_matches_730, euro_wr_shrunk
    """
    if hist_df.empty or "league" not in hist_df.columns:
        return pd.DataFrame(columns=["team","euro_matches_365","euro_matches_730","euro_wr_shrunk"])

    df = hist_df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df = df[df["league"].astype(str).apply(is_uefa)]
    if df.empty:
        return pd.DataFrame(columns=["team","euro_matches_365","euro_matches_730","euro_wr_shrunk"])

    h = df[["date","home_team","home_goals","away_goals"]].rename(
        columns={"home_team":"team","home_goals":"gf","away_goals":"ga"})
    a = df[["date","away_team","home_goals","away_goals"]].rename(
        columns={"away_team":"team","away_goals":"gf","home_goals":"ga"})
    long = pd.concat([h,a], ignore_index=True)
    long["win"] = (pd.to_numeric(long["gf"], errors="coerce") > pd.to_numeric(long["ga"], errors="coerce")).astype(int)

    max_date = long["date"].max()
    cutoff_365 = max_date - pd.Timedelta(days=365)
    cutoff_730 = max_date - pd.Timedelta(days=730)

    def agg(team_df):
        m365 = int((team_df["date"] >= cutoff_365).sum())
        m730 = int((team_df["date"] >= cutoff_730).sum())
        n = len(team_df)
        wr = float(team_df["win"].mean()) if n>0 else np.nan
        p0, alpha = 1.0/3.0, 5.0
        wr_shrunk = (wr*n + p0*alpha) / (n + alpha) if n>0 and np.isfinite(wr) else p0
        return pd.Series({"euro_matches_365":m365, "euro_matches_730":m730, "euro_wr_shrunk":wr_shrunk})

    exp = long.groupby("team", as_index=False).apply(agg)
    if isinstance(exp.index, pd.MultiIndex):
        exp = exp.reset_index(level=0, drop=True).reset_index().rename(columns={"index":"team"})
    return exp

def fallback_lsi_from_hist(hist_df: pd.DataFrame):
    """If SPI doesn't give a usable league LSI, proxy from HIST domestic goal diff."""
    if hist_df.empty:
        return pd.DataFrame(columns=["league","lsi"])
    df = hist_df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    if "league" not in df.columns: df["league"] = "GLOBAL"
    # domestic only
    df = df[~df["league"].astype(str).apply(is_uefa)]
    if df.empty:
        return pd.DataFrame(columns=["league","lsi"])
    h = df[["league","home_team","home_goals","away_goals"]].rename(
        columns={"home_team":"team","home_goals":"gf","away_goals":"ga"})
    a = df[["league","away_team","home_goals","away_goals"]].rename(
        columns={"away_team":"team","away_goals":"gf","home_goals":"ga"})
    long = pd.concat([h,a], ignore_index=True)
    long["gd"] = pd.to_numeric(long["gf"], errors="coerce") - pd.to_numeric(long["ga"], errors="coerce")
    return long.groupby("league", as_index=False)["gd"].mean().rename(columns={"gd":"lsi"})

def main():
    up = safe_read(UP)
    if up.empty:
        print(f"[WARN] {UP} missing/empty; nothing to engineer."); return
    if "league" not in up.columns: up["league"] = "GLOBAL"

    spi_df = safe_read(SPI)
    hist   = safe_read(HIST, ["date","home_team","away_team","home_goals","away_goals","league"])

    # Build TSI & LSI from SPI (and fallback)
    team_spi, league_spi_spi = parse_spi(spi_df)

    # Build domestic mapping (team -> dom_league) from HIST
    dom_map = build_domestic_mapping(hist)

    # If SPI didn't have per-league LSI, try two fallbacks:
    #  (1) infer league LSI by averaging team TSI grouped by DOMESTIC league (using dom_map)
    #  (2) proxy from HIST domestic goal-diff
    if league_spi_spi.empty and (not team_spi.empty) and (not dom_map.empty):
        tmp = team_spi.merge(dom_map, left_on="team", right_on="team", how="inner")
        league_spi_by_team = tmp.groupby("dom_league", as_index=False)["tsi"].mean() \
                                .rename(columns={"dom_league":"league","tsi":"lsi"})
        league_spi = league_spi_by_team
    else:
        league_spi = league_spi_spi

    if league_spi.empty:
        league_spi = fallback_lsi_from_hist(hist)

    # UEFA experience (home/away)
    exp = build_uefa_experience(hist)

    # Merge into UPCOMING
    df = up.copy()

    # Merge domestic league for home/away
    if not dom_map.empty:
        df = df.merge(dom_map.rename(columns={"team":"home_team","dom_league":"engine_home_dom_league"}),
                      on="home_team", how="left")
        df = df.merge(dom_map.rename(columns={"team":"away_team","dom_league":"engine_away_dom_league"}),
                      on="away_team", how="left")
    else:
        df["engine_home_dom_league"] = np.nan
        df["engine_away_dom_league"] = np.nan

    # Merge league strength for domestic leagues
    if not league_spi.empty:
        lsi_home = league_spi.rename(columns={"league":"engine_home_dom_league",
                                              "lsi":"engine_home_league_strength"})
        lsi_away = league_spi.rename(columns={"league":"engine_away_dom_league",
                                              "lsi":"engine_away_league_strength"})
        df = df.merge(lsi_home, on="engine_home_dom_league", how="left")
        df = df.merge(lsi_away, on="engine_away_dom_league", how="left")
    else:
        df["engine_home_league_strength"] = np.nan
        df["engine_away_league_strength"] = np.nan

    # Team strength from SPI
    if not team_spi.empty:
        hspi = team_spi.rename(columns={"team":"home_team","tsi":"engine_home_team_spi"})
        aspi = team_spi.rename(columns={"team":"away_team","tsi":"engine_away_team_spi"})
        df = df.merge(hspi, on="home_team", how="left").merge(aspi, on="away_team", how="left")
    else:
        df["engine_home_team_spi"] = np.nan
        df["engine_away_team_spi"] = np.nan

    # Diffs
    df["engine_lsi_diff"] = df.get("engine_home_league_strength", np.nan) - df.get("engine_away_league_strength", np.nan)
    df["engine_tsi_diff"] = df.get("engine_home_team_spi", np.nan)        - df.get("engine_away_team_spi", np.nan)

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
    else:
        df["engine_home_euro_matches_365"] = np.nan
        df["engine_home_euro_matches_730"] = np.nan
        df["engine_home_euro_wr_shrunk"]   = np.nan
        df["engine_away_euro_matches_365"] = np.nan
        df["engine_away_euro_matches_730"] = np.nan
        df["engine_away_euro_wr_shrunk"]   = np.nan

    # Clean up: dom league helper columns can be kept (analysts like to see them),
    # but if you want to hide them, comment out the next two lines.
    # df = df.drop(columns=["engine_home_dom_league","engine_away_dom_league"], errors="ignore")

    df.to_csv(UP, index=False)
    print(f"[OK] engineered tournament extras merged → {UP} (rows={len(df)})")

if __name__ == "__main__":
    main()