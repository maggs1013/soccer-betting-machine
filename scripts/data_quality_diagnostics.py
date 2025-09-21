# scripts/data_quality_diagnostics.py
# Scores coverage for the next-7-day dataset:
#  - odds coverage
#  - xG coverage (home_xg/away_xg)
#  - priors coverage (gk/setpiece/crowd)
#  - injuries & lineup flags
#  - travel & dates
# Writes data/DATA_QUALITY_REPORT.csv and prints a summary.

import os
import pandas as pd
import numpy as np

DATA = "data"
UP7 = os.path.join(DATA, "UPCOMING_7D_enriched.csv")
OUT = os.path.join(DATA, "DATA_QUALITY_REPORT.csv")

def safe_read(p):
    if not os.path.exists(p):
        return pd.DataFrame()
    try:
        return pd.read_csv(p)
    except Exception:
        return pd.DataFrame()

def pct(n, d):
    return 0.0 if d == 0 else round(100.0 * n / d, 1)

def main():
    df = safe_read(UP7)
    if df.empty:
        print("[WARN] UPCOMING_7D_enriched.csv is empty — writing header-only report.")
        pd.DataFrame(columns=["metric","count","total","percent"]).to_csv(OUT, index=False)
        return

    total = len(df)

    # Booleans for coverage
    # Odds
    odds_ok = df["home_odds_dec"].notna() & df["draw_odds_dec"].notna() & df["away_odds_dec"].notna() if \
        {"home_odds_dec","draw_odds_dec","away_odds_dec"}.issubset(df.columns) else pd.Series([False]*total)

    # xG (hybrid)
    xg_ok = df["home_xg"].notna() & df["away_xg"].notna() if {"home_xg","away_xg"}.issubset(df.columns) else pd.Series([False]*total)

    # Priors
    pri_gk = df["home_gk_rating"].notna() & df["away_gk_rating"].notna() if {"home_gk_rating","away_gk_rating"}.issubset(df.columns) else pd.Series([False]*total)
    pri_sp = df["home_setpiece_rating"].notna() & df["away_setpiece_rating"].notna() if {"home_setpiece_rating","away_setpiece_rating"}.issubset(df.columns) else pd.Series([False]*total)
    pri_crowd = df["crowd_index"].notna() if "crowd_index" in df.columns else pd.Series([False]*total)

    # Injuries / lineups
    inj_ok = df["home_injury_index"].notna() & df["away_injury_index"].notna() if {"home_injury_index","away_injury_index"}.issubset(df.columns) else pd.Series([False]*total)
    lu_flags = {"home_key_att_out","home_key_def_out","home_keeper_changed","away_key_att_out","away_key_def_out","away_keeper_changed"}
    lu_ok = pd.Series([False]*total)
    if lu_flags.issubset(df.columns):
        # Consider coverage "ok" if flags exist (values may be 0 or 1)
        lu_ok = pd.Series([True]*total)

    # Travel / dates
    travel_ok = df["away_travel_km"].notna() if "away_travel_km" in df.columns else pd.Series([False]*total)
    date_ok = df["date"].notna() if "date" in df.columns else pd.Series([False]*total)

    rows = [
        {"metric":"fixtures_total","count":total,"total":total,"percent":100.0},
        {"metric":"odds_coverage","count":odds_ok.sum(),"total":total,"percent":pct(odds_ok.sum(), total)},
        {"metric":"xg_coverage","count":xg_ok.sum(),"total":total,"percent":pct(xg_ok.sum(), total)},
        {"metric":"priors_gk","count":pri_gk.sum(),"total":total,"percent":pct(pri_gk.sum(), total)},
        {"metric":"priors_setpiece","count":pri_sp.sum(),"total":total,"percent":pct(pri_sp.sum(), total)},
        {"metric":"priors_crowd","count":pri_crowd.sum(),"total":total,"percent":pct(pri_crowd.sum(), total)},
        {"metric":"injuries_index","count":inj_ok.sum(),"total":total,"percent":pct(inj_ok.sum(), total)},
        {"metric":"lineup_flags_present","count":lu_ok.sum(),"total":total,"percent":pct(lu_ok.sum(), total)},
        {"metric":"travel_km","count":travel_ok.sum(),"total":total,"percent":pct(travel_ok.sum(), total)},
        {"metric":"date_present","count":date_ok.sum(),"total":total,"percent":pct(date_ok.sum(), total)},
    ]

    rep = pd.DataFrame(rows)
    rep.to_csv(OUT, index=False)

    print("\n=== DATA QUALITY (next 7 days) ===")
    for r in rows:
        print(f"{r['metric']:>22s}: {r['count']:>4d}/{r['total']:<4d}  ({r['percent']:>5.1f}%)")
    print(f"[OK] wrote {OUT}")

if __name__ == "__main__":
    main()