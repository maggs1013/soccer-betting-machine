# scripts/merge_statsbomb_into_xg.py
# Fill gaps in xg_metrics_hybrid.csv using StatsBomb totals (per-90 proxy) and print coverage.

import os
import pandas as pd

DATA = "data"
HYB  = os.path.join(DATA, "xg_metrics_hybrid.csv")
SB   = os.path.join(DATA, "xg_statsbomb.csv")

def main():
    if not os.path.exists(HYB):
        print("[INFO] hybrid missing; skip StatsBomb merge."); return
    hyb = pd.read_csv(HYB)
    if hyb.empty or "team" not in hyb.columns:
        print("[INFO] hybrid empty or lacks 'team'; skip."); return
    if not os.path.exists(SB):
        print("[INFO] xg_statsbomb.csv missing; skip."); return
    sb = pd.read_csv(SB)
    if sb.empty or "team" not in sb.columns:
        print("[INFO] StatsBomb file empty; skip."); return

    sb["xg_sb_p90"]  = pd.to_numeric(sb.get("xg_sb", pd.Series()),  errors="coerce") / 34.0
    sb["xga_sb_p90"] = pd.to_numeric(sb.get("xga_sb", pd.Series()), errors="coerce") / 34.0
    sb_small = sb.groupby("team", as_index=False)[["xg_sb_p90","xga_sb_p90"]].mean()

    merged = hyb.merge(sb_small, on="team", how="left")

    for c in ["xg_hybrid","xga_hybrid","xgd90_hybrid"]:
        if c not in merged.columns: merged[c] = pd.NA

    pre_nan_xg  = merged["xg_hybrid"].isna().sum()
    pre_nan_xga = merged["xga_hybrid"].isna().sum()
    pre_nan_xgd = merged["xgd90_hybrid"].isna().sum()

    fill_xg  = merged["xg_hybrid"].isna()  & merged["xg_sb_p90"].notna()
    fill_xga = merged["xga_hybrid"].isna() & merged["xga_sb_p90"].notna()
    fill_xgd = merged["xgd90_hybrid"].isna() & merged["xg_sb_p90"].notna() & merged["xga_sb_p90"].notna()

    merged.loc[fill_xg,  "xg_hybrid"]    = merged.loc[fill_xg,  "xg_sb_p90"]
    merged.loc[fill_xga, "xga_hybrid"]   = merged.loc[fill_xga, "xga_sb_p90"]
    merged.loc[fill_xgd, "xgd90_hybrid"] = merged.loc[fill_xgd, "xg_sb_p90"] - merged.loc[fill_xgd, "xga_sb_p90"]

    post_nan_xg  = merged["xg_hybrid"].isna().sum()
    post_nan_xga = merged["xga_hybrid"].isna().sum()
    post_nan_xgd = merged["xgd90_hybrid"].isna().sum()

    merged.to_csv(HYB, index=False)

    print("\n=== StatsBomb merge coverage ===")
    print(f"Rows before fill (NaNs)  xg: {pre_nan_xg}  xga: {pre_nan_xga}  xgd90: {pre_nan_xgd}")
    print(f"Rows after  fill (NaNs)  xg: {post_nan_xg}  xga: {post_nan_xga}  xgd90: {post_nan_xgd}")
    print(f"Filled from StatsBomb    xg: {pre_nan_xg - post_nan_xg} | xga: {pre_nan_xga - post_nan_xga} | xgd90: {pre_nan_xgd - post_nan_xgd}")
    print(f"[OK] StatsBomb merged into {HYB}. rows={len(merged)}")

if __name__ == "__main__":
    main()