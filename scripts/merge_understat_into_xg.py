# scripts/merge_understat_into_xg.py
# Fill gaps in data/xg_metrics_hybrid.csv using Understat team totals (Big-5).
# We DO NOT override existing hybrid values; we only fill where hybrid is NaN.

import os
import pandas as pd

DATA = "data"
HYB  = os.path.join(DATA, "xg_metrics_hybrid.csv")
US   = os.path.join(DATA, "xg_understat.csv")

def main():
    if not os.path.exists(HYB):
        print("[INFO] hybrid missing; skipping Understat merge.")
        return

    hyb = pd.read_csv(HYB)
    if hyb.empty or "team" not in hyb.columns:
        print("[INFO] hybrid empty or missing 'team'; skip.")
        return

    if not os.path.exists(US):
        print("[INFO] xg_understat.csv missing; skip Understat merge.")
        return
    us = pd.read_csv(US)
    if us.empty or "team" not in us.columns:
        print("[INFO] Understat file empty; skip.")
        return

    # Prepare Understat per-match proxies (divide by a proxy matches if needed).
    # Understat returns totals; we’ll convert to per-90 proxy using a nominal 34 match season if no matches are known.
    us["xg_u_p90"]  = pd.to_numeric(us["xg_u"],  errors="coerce") / 34.0
    us["xga_u_p90"] = pd.to_numeric(us["xga_u"], errors="coerce") / 34.0
    us_small = us[["team","xg_u_p90","xga_u_p90"]].copy()

    merged = hyb.merge(us_small, on="team", how="left")

    # Ensure hybrid columns exist
    for c in ["xg_hybrid","xga_hybrid","xgd90_hybrid"]:
        if c not in merged.columns: merged[c] = pd.NA

    # Fill only where missing
    fill_xg  = merged["xg_hybrid"].isna()  & merged["xg_u_p90"].notna()
    fill_xga = merged["xga_hybrid"].isna() & merged["xga_u_p90"].notna()
    fill_xgd = merged["xgd90_hybrid"].isna() & merged["xg_u_p90"].notna() & merged["xga_u_p90"].notna()

    merged.loc[fill_xg,  "xg_hybrid"]    = merged.loc[fill_xg,  "xg_u_p90"]
    merged.loc[fill_xga, "xga_hybrid"]   = merged.loc[fill_xga, "xga_u_p90"]
    merged.loc[fill_xgd, "xgd90_hybrid"] = merged.loc[fill_xgd, "xg_u_p90"] - merged.loc[fill_xgd, "xga_u_p90"]

    merged.to_csv(HYB, index=False)
    print(f"[OK] Understat merged into {HYB}. rows={len(merged)}")

if __name__ == "__main__":
    main()