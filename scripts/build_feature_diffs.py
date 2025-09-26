#!/usr/bin/env python3
"""
build_feature_diffs.py — produce model-ready diffs from enriched table
Reads:  data/UPCOMING_7D_enriched.csv
Writes: data/UPCOMING_features_matrix.csv
"""

import os, math
import pandas as pd
import numpy as np

DATA = "data"
ENR = os.path.join(DATA, "UPCOMING_7D_enriched.csv")
OUT = os.path.join(DATA, "UPCOMING_features_matrix.csv")

def imp_prob(odds):
    try:
        x = float(odds)
        return 0.0 if x <= 1e-9 else 1.0/x
    except Exception:
        return np.nan

def main():
    if not os.path.exists(ENR):
        pd.DataFrame().to_csv(OUT, index=False); print("features: no enriched"); return
    df = pd.read_csv(ENR)

    feats = pd.DataFrame()
    req = ["fixture_id","date","home_team","away_team"]
    for c in req:
        if c in df.columns: feats[c] = df[c]

    # H2H implied probs & simple vig
    for c in ["home_odds_dec","draw_odds_dec","away_odds_dec"]:
        if c in df.columns: feats[c.replace("_odds_dec","_p")] = df[c].apply(imp_prob)
    if {"home_p","draw_p","away_p"}.issubset(feats.columns):
        feats["h2h_vig_sum"] = feats[["home_p","draw_p","away_p"]].sum(axis=1)

    # OU implied probs if present
    if {"ou_over_price","ou_under_price"}.issubset(df.columns):
        feats["ou_over_p"]  = df["ou_over_price"].apply(imp_prob)
        feats["ou_under_p"] = df["ou_under_price"].apply(imp_prob)
        feats["ou_total"]   = df.get("ou_main_total", np.nan)

    # BTTS
    if "btts_yes_price" in df.columns:
        feats["btts_yes_p"] = df["btts_yes_price"].apply(imp_prob)
    if "btts_no_price" in df.columns:
        feats["btts_no_p"] = df["btts_no_price"].apply(imp_prob)

    # Availability / injuries
    for c in ["home_avail","away_avail","home_injury_index","away_injury_index"]:
        if c in df.columns: feats[c] = df[c]
    if {"home_avail","away_avail"}.issubset(feats.columns):
        feats["avail_diff"] = feats["home_avail"] - feats["away_avail"]
    if {"home_injury_index","away_injury_index"}.issubset(feats.columns):
        feats["injury_index_diff"] = feats["home_injury_index"] - feats["away_injury_index"]

    # SPI rank diff
    if {"home_spi_rank","away_spi_rank"}.issubset(df.columns):
        feats["spi_rank_diff"] = df["home_spi_rank"].fillna(0) - df["away_spi_rank"].fillna(0)

    # FBref generic: if columns exist like home_xxx and away_xxx, create diffs
    home_cols = [c for c in df.columns if c.startswith("home_")]
    for hc in home_cols:
        ac = "away_" + hc[len("home_"):]
        if ac in df.columns and hc not in ("home_team", "home_league", "home_season"):
            feat_name = hc.replace("home_","diff_")
            feats[feat_name] = df[hc].astype(float).fillna(0) - df[ac].astype(float).fillna(0)

    feats.to_csv(OUT, index=False)
    print(f"features: wrote {OUT} rows={len(feats)}")

if __name__ == "__main__":
    main()