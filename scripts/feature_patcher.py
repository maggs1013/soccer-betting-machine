#!/usr/bin/env python3
"""
feature_patcher.py — diffs, coherence, interactions (FINAL)
Inputs:  data/UPCOMING_7D_enriched.csv
Outputs: data/UPCOMING_7D_features.csv

Adds (best-effort, schema-agnostic):
- Core diffs: spi_rank_diff, injury_index_diff, availability_diff
- Keeper/Passing diffs if present (PSxG prevented, pass%)
- Rolling-form diffs: last5/last10 (goals_for/goals_against/points) if present
- Lineups depth: experienced starters pct diff
- Market features: dispersion, has_*, btts_price_gap, ou_main_total, spread diffs
- Timing features: has_opening_odds, has_closing_odds
- Interactions: rank_diff×injury_diff, availability_diff×home_travel_km
- SPI uncertainty: home/away spi_ci_width + diff (if present)
- Tournament hooks: is_neutral, must_win (if present)

Safe: if a source column is missing, the feature is NaN; never crashes.
"""

import os
import numpy as np
import pandas as pd

DATA = "data"
SRC  = os.path.join(DATA, "UPCOMING_7D_enriched.csv")
OUT  = os.path.join(DATA, "UPCOMING_7D_features.csv")

def safe_read(path):
    if not os.path.exists(path): return pd.DataFrame()
    try: return pd.read_csv(path)
    except Exception: return pd.DataFrame()

def mk_id(r):
    d = str(r.get("date","NA")).replace("-","").replace("T","_").replace(":","")
    h = str(r.get("home_team","NA")).strip().lower().replace(" ","_")
    a = str(r.get("away_team","NA")).strip().lower().replace(" ","_")
    return f"{d}__{h}__vs__{a}"

def num(s):
    try: return pd.to_numeric(s, errors="coerce")
    except Exception: return pd.Series(np.nan, index=s.index)

def first_or_nan(cols, df):
    return df[cols[0]] if cols else pd.Series(np.nan, index=df.index)

def main():
    up = safe_read(SRC)
    if up.empty:
        pd.DataFrame(columns=["fixture_id"]).to_csv(OUT, index=False)
        print("feature_patcher: no enriched; wrote header-only")
        return

    if "fixture_id" not in up.columns:
        up["fixture_id"] = up.apply(mk_id, axis=1)

    feats = pd.DataFrame({"fixture_id": up["fixture_id"]})

    # ---------- Core diffs ----------
    if {"home_spi_rank","away_spi_rank"}.issubset(up.columns):
        feats["spi_rank_diff"] = num(up["home_spi_rank"]) - num(up["away_spi_rank"])

    if {"home_injury_index","away_injury_index"}.issubset(up.columns):
        feats["injury_index_diff"] = num(up["home_injury_index"]) - num(up["away_injury_index"])

    if {"home_avail","away_avail"}.issubset(up.columns):
        feats["availability_diff"] = num(up["home_avail"]) - num(up["away_avail"])

    # SPI uncertainty (if enrichment mapped these)
    if {"home_spi_ci_width","away_spi_ci_width"}.issubset(up.columns):
        feats["spi_ci_width_home"] = num(up["home_spi_ci_width"])
        feats["spi_ci_width_away"] = num(up["away_spi_ci_width"])
        feats["spi_ci_width_diff"] = feats["spi_ci_width_home"] - feats["spi_ci_width_away"]

    # ---------- Keeper / Passing diffs (heuristic column find) ----------
    hkp = [c for c in up.columns if c.startswith("home_") and "psxg" in c.lower() and "prevent" in c.lower()]
    akp = [c for c in up.columns if c.startswith("away_") and "psxg" in c.lower() and "prevent" in c.lower()]
    if hkp and akp:
        feats["keeper_psxg_prevented_diff"] = num(up[hkp[0]]) - num(up[akp[0]])

    hpass = [c for c in up.columns if c.startswith("home_") and ("pass_pct" in c.lower() or "passing" in c.lower())]
    apass = [c for c in up.columns if c.startswith("away_") and ("pass_pct" in c.lower() or "passing" in c.lower())]
    if hpass and apass:
        feats["passing_accuracy_diff"] = num(up[hpass[0]]) - num(up[apass[0]])

    # ---------- Rolling form diffs (if present) ----------
    for n in (5, 10):
        gf_h = f"home_last{n}_goals_for";    gf_a = f"away_last{n}_goals_for"
        ga_h = f"home_last{n}_goals_against";ga_a = f"away_last{n}_goals_against"
        pts_h= f"home_last{n}_points";       pts_a= f"away_last{n}_points"
        if {gf_h, gf_a}.issubset(up.columns):
            feats[f"last{n}_gf_diff"] = num(up[gf_h]) - num(up[gf_a])
        if {ga_h, ga_a}.issubset(up.columns):
            feats[f"last{n}_ga_diff"] = num(up[ga_h]) - num(up[ga_a])
        if {pts_h, pts_a}.issubset(up.columns):
            feats[f"last{n}_pts_diff"] = num(up[pts_h]) - num(up[pts_a])

    # ---------- Lineups depth ----------
    if {"home_exp_starters_pct","away_exp_starters_pct"}.issubset(up.columns):
        feats["exp_starters_pct_diff"] = num(up["home_exp_starters_pct"]) - num(up["away_exp_starters_pct"])

    # ---------- Market & coherence ----------
    if "bookmaker_count" in up.columns:
        feats["market_dispersion"] = num(up["bookmaker_count"])

    for flag in ["has_ou","has_btts","has_spread"]:
        if flag in up.columns:
            feats[flag] = up[flag]

    if {"btts_yes_price","btts_no_price"}.issubset(up.columns):
        feats["btts_price_gap"] = (num(up["btts_yes_price"]) - num(up["btts_no_price"])).abs()

    if "ou_main_total" in up.columns:
        feats["ou_main_total"] = num(up["ou_main_total"])

    # spreads
    if {"spread_home_line","spread_away_line"}.issubset(up.columns):
        feats["spread_line_diff"] = num(up["spread_home_line"]) - num(up["spread_away_line"])
    if {"spread_home_price","spread_away_price"}.issubset(up.columns):
        feats["spread_price_diff"] = num(up["spread_home_price"]) - num(up["spread_away_price"])

    # timing flags
    if "has_opening_odds" in up.columns: feats["has_opening_odds"] = up["has_opening_odds"]
    if "has_closing_odds" in up.columns: feats["has_closing_odds"] = up["has_closing_odds"]

    # ---------- Interactions ----------
    if {"spi_rank_diff","injury_index_diff"}.issubset(feats.columns):
        feats["rank_x_injury"] = feats["spi_rank_diff"] * feats["injury_index_diff"]
    if "availability_diff" in feats.columns and "home_travel_km" in up.columns:
        feats["avail_x_travel"] = feats["availability_diff"] * num(up["home_travel_km"])

    # ---------- Tournament hooks ----------
    for col in ["is_neutral","must_win"]:
        if col in up.columns:
            feats[col] = up[col]

    # ---------- Context passthrough ----------
    for col in ["date","league","home_team","away_team"]:
        if col in up.columns:
            feats[col] = up[col]

    feats.to_csv(OUT, index=False)
    print(f"feature_patcher: wrote {OUT} rows={len(feats)} with {feats.shape[1]-1} feature columns")

if __name__ == "__main__":
    main()