#!/usr/bin/env python3
"""
consistency_checks_plus.py
- Basic sanity checks across H2H vs OU/BTTS
Writes: reports/CONSISTENCY_CHECKS_PLUS.md
"""

import os, math
import pandas as pd
import numpy as np

DATA = "data"
REPORTS = "reports"
ENR = os.path.join(DATA, "UPCOMING_7D_enriched.csv")
OUT = os.path.join(REPORTS, "CONSISTENCY_CHECKS_PLUS.md")

def imp_prob(x):
    try:
        v = float(x);  return 0.0 if v<=0 else 1.0/v
    except: return np.nan

def main():
    os.makedirs(REPORTS, exist_ok=True)
    if not os.path.exists(ENR):
        with open(OUT,"w") as f: f.write("# CONSISTENCY_CHECKS_PLUS\n- No enriched file.\n"); return
    df = pd.read_csv(ENR)
    lines = ["# CONSISTENCY_CHECKS_PLUS"]

    # H2H vig sanity
    if {"home_odds_dec","draw_odds_dec","away_odds_dec"}.issubset(df.columns):
        p_sum = df["home_odds_dec"].apply(imp_prob) + df["draw_odds_dec"].apply(imp_prob) + df["away_odds_dec"].apply(imp_prob)
        too_high = (p_sum > 1.25).sum()
        too_low  = (p_sum < 0.95).sum()
        lines.append(f"- H2H vig sum > 1.25 count: {too_high}")
        lines.append(f"- H2H vig sum < 0.95 count: {too_low}")

    # Presence stats for OU/BTTS
    lines.append(f"- has_ou count: {int(df.get('has_ou',0).sum())}")
    lines.append(f"- has_btts count: {int(df.get('has_btts',0).sum())}")
    lines.append(f"- has_spread count: {int(df.get('has_spread',0).sum())}")

    with open(OUT,"w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"consistency_plus: wrote {OUT}")

if __name__ == "__main__":
    main()