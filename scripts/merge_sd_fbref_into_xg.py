# scripts/merge_sd_fbref_into_xg.py
# Fill gaps in xg_metrics_hybrid.csv using FBref xG from soccerdata.

import os
import pandas as pd

DATA = "data"
HYB_PATH = os.path.join(DATA, "xg_metrics_hybrid.csv")
FBREF_PATH = os.path.join(DATA, "sd_fbref_team_stats.csv")

def main():
    if not os.path.exists(HYB_PATH):
        print("[INFO] xg_metrics_hybrid.csv not found; skip FBref merge.")
        return

    hyb = pd.read_csv(HYB_PATH)
    if hyb.empty or "team" not in hyb.columns:
        print("[INFO] hybrid file empty or missing 'team'; skip.")
        return

    if not os.path.exists(FBREF_PATH):
        print("[INFO] sd_fbref_team_stats.csv not present; skip FBref fallback.")
        return

    fb = pd.read_csv(FBREF_PATH)
    if fb.empty:
        print("[INFO] FBref file empty; skip.")
        return

    # Ensure required columns exist (fetch_soccerdata now enforces these)
    for c in ["team", "xg_for_fbref", "xg_against_fbref", "matches"]:
        if c not in fb.columns:
            fb[c] = pd.NA

    # Compute per-90 proxies when matches available
    fb["xg_for_p90_fbref"] = pd.to_numeric(fb["xg_for_fbref"], errors="coerce")
    fb["xg_against_p90_fbref"] = pd.to_numeric(fb["xg_against_fbref"], errors="coerce")

    m = pd.to_numeric(fb["matches"], errors="coerce")
    has_m = m.notna() & (m > 0)
    fb.loc[has_m, "xg_for_p90_fbref"] = fb.loc[has_m, "xg_for_p90_fbref"] / m.loc[has_m]
    fb.loc[has_m, "xg_against_p90_fbref"] = fb.loc[has_m, "xg_against_p90_fbref"] / m.loc[has_m]

    fb_small = fb[["team","xg_for_p90_fbref","xg_against_p90_fbref"]].copy()

    merged = hyb.merge(fb_small, on="team", how="left")

    # Only fill where hybrid is missing
    if "xg_hybrid" not in merged.columns:
        merged["xg_hybrid"] = pd.NA
    if "xga_hybrid" not in merged.columns:
        merged["xga_hybrid"] = pd.NA
    if "xgd90_hybrid" not in merged.columns:
        merged["xgd90_hybrid"] = pd.NA

    fill_xg  = merged["xg_hybrid"].isna() & merged["xg_for_p90_fbref"].notna()
    fill_xga = merged["xga_hybrid"].isna() & merged["xg_against_p90_fbref"].notna()
    fill_xgd = merged["xgd90_hybrid"].isna() & merged["xg_for_p90_fbref"].notna() & merged["xg_against_p90_fbref"].notna()

    merged.loc[fill_xg,  "xg_hybrid"]    = merged.loc[fill_xg,  "xg_for_p90_fbref"]
    merged.loc[fill_xga, "xga_hybrid"]   = merged.loc[fill_xga, "xg_against_p90_fbref"]
    merged.loc[fill_xgd, "xgd90_hybrid"] = merged.loc[fill_xgd, "xg_for_p90_fbref"] - merged.loc[fill_xgd, "xg_against_p90_fbref"]

    merged.to_csv(HYB_PATH, index=False)
    print(f"[OK] Merged FBref fallback into {HYB_PATH}. Rows: {len(merged)}")

if __name__ == "__main__":
    main()