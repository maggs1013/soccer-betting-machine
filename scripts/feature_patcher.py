#!/usr/bin/env python3
"""
feature_patcher.py — diffs, coherence & interactions (FINAL)

Inputs:
  data/UPCOMING_7D_enriched.csv
Outputs:
  data/UPCOMING_7D_features.csv

What this builds (best-effort, schema-agnostic; safe if columns missing):
- Core diffs: spi_rank_diff, injury_index_diff, availability_diff
- Lineups depth: exp_starters_pct_diff
- Rolling-form diffs (if present): last5/last10 gf/ga/pts (kept for backward compat)
- NEW FBref diffs: passing_accuracy_diff, keeper_psxg_prevented_diff,
                  sca90_diff, gca90_diff, pressures90_diff, tackles90_diff,
                  setpiece_share_diff
- Market features: market_dispersion, has_*, btts_price_gap, ou_main_total,
                   spread_line_diff, spread_price_diff
- Timing flags: has_opening_odds, has_closing_odds
- SPI uncertainty: home/away spi_ci_width, spi_ci_width_diff
- Interactions:
    rank_x_injury = spi_rank_diff * injury_index_diff
    avail_x_travel = availability_diff * home_travel_km
    unc_x_ou = spi_ci_width_diff * ou_main_total
- Context passthrough: date, league, home_team, away_team
"""

import os
import numpy as np
import pandas as pd

DATA = "data"
SRC  = os.path.join(DATA, "UPCOMING_7D_enriched.csv")
OUT  = os.path.join(DATA, "UPCOMING_7D_features.csv")

def safe_read(path):
    if not os.path.exists(path): 
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()

def mk_id(r):
    d = str(r.get("date","NA")).replace("-","").replace("T","_").replace(":","")
    h = str(r.get("home_team","NA")).strip().lower().replace(" ","_")
    a = str(r.get("away_team","NA")).strip().lower().replace(" ","_")
    return f"{d}__{h}__vs__{a}"

def num(s):
    """Coerce Series or ndarray to numeric; pass-through for missing."""
    try:
        return pd.to_numeric(s, errors="coerce")
    except Exception:
        return s

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

    # ---------- SPI uncertainty ----------
    if {"home_spi_ci_width","away_spi_ci_width"}.issubset(up.columns):
        feats["spi_ci_width_home"] = num(up["home_spi_ci_width"])
        feats["spi_ci_width_away"] = num(up["away_spi_ci_width"])
        feats["spi_ci_width_diff"] = feats["spi_ci_width_home"] - feats["spi_ci_width_away"]

    # ---------- Lineups depth ----------
    if {"home_exp_starters_pct","away_exp_starters_pct"}.issubset(up.columns):
        feats["exp_starters_pct_diff"] = num(up["home_exp_starters_pct"]) - num(up["away_exp_starters_pct"])

    # ---------- FBref keeper / passing diffs ----------
    if {"home_gk_psxg_prevented","away_gk_psxg_prevented"}.issubset(up.columns):
        feats["keeper_psxg_prevented_diff"] = num(up["home_gk_psxg_prevented"]) - num(up["away_gk_psxg_prevented"])

    if {"home_pass_pct","away_pass_pct"}.issubset(up.columns):
        feats["passing_accuracy_diff"] = num(up["home_pass_pct"]) - num(up["away_pass_pct"])

    # ---------- GCA/SCA diffs ----------
    if {"home_sca90","away_sca90"}.issubset(up.columns):
        feats["sca90_diff"] = num(up["home_sca90"]) - num(up["away_sca90"])
    if {"home_gca90","away_gca90"}.issubset(up.columns):
        feats["gca90_diff"] = num(up["home_gca90"]) - num(up["away_gca90"])

    # ---------- Defensive action diffs ----------
    if {"home_pressures90","away_pressures90"}.issubset(up.columns):
        feats["pressures90_diff"] = num(up["home_pressures90"]) - num(up["away_pressures90"])
    if {"home_tackles90","away_tackles90"}.issubset(up.columns):
        feats["tackles90_diff"] = num(up["home_tackles90"]) - num(up["away_tackles90"])

    # ---------- Set-piece share diff ----------
    if {"home_setpiece_share","away_setpiece_share"}.issubset(up.columns):
        feats["setpiece_share_diff"] = num(up["home_setpiece_share"]) - num(up["away_setpiece_share"])

    # ---------- Rolling form (backward compat; keep if present) ----------
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

    if {"spread_home_line","spread_away_line"}.issubset(up.columns):
        feats["spread_line_diff"] = num(up["spread_home_line"]) - num(up["spread_away_line"])
    if {"spread_home_price","spread_away_price"}.issubset(up.columns):
        feats["spread_price_diff"] = num(up["spread_home_price"]) - num(up["spread_away_price"])

    if "has_opening_odds" in up.columns: 
        feats["has_opening_odds"] = up["has_opening_odds"]
    if "has_closing_odds" in up.columns: 
        feats["has_closing_odds"] = up["has_closing_odds"]

    # ---------- Interactions ----------
    if {"spi_rank_diff","injury_index_diff"}.issubset(feats.columns):
        feats["rank_x_injury"] = feats["spi_rank_diff"] * feats["injury_index_diff"]

    if "availability_diff" in feats.columns and "home_travel_km" in up.columns:
        feats["avail_x_travel"] = feats["availability_diff"] * num(up["home_travel_km"])

    # NEW: uncertainty × OU interaction
    if "spi_ci_width_diff" in feats.columns and "ou_main_total" in feats.columns:
        feats["unc_x_ou"] = feats["spi_ci_width_diff"] * feats["ou_main_total"]

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