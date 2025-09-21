# scripts/seed_teams_master_from_sb.py
# Seeds data/teams_master.csv using team_statsbomb_features.csv:
#  - gk_rating from psxg_minus_goals_sb (better shot-stopping -> higher)
#  - setpiece_rating from setpiece_xg share
#  - crowd_index default 0.70 (can be hand-tuned)
#
# This provides non-flat priors so enrichment has signal immediately.

import os, pandas as pd, numpy as np

DATA = "data"
IN   = os.path.join(DATA, "team_statsbomb_features.csv")
OUT  = os.path.join(DATA, "teams_master.csv")

def clamp(v, lo, hi):
    try: v = float(v)
    except: return (lo+hi)/2
    return max(lo, min(hi, v))

def main():
    if not os.path.exists(IN):
        print("[MISS] team_statsbomb_features.csv")
        pd.DataFrame(columns=["team","gk_rating","setpiece_rating","crowd_index"]).to_csv(OUT, index=False)
        return

    df = pd.read_csv(IN)
    if df.empty or "team" not in df.columns:
        print("[EMPTY] team_statsbomb_features.csv")
        pd.DataFrame(columns=["team","gk_rating","setpiece_rating","crowd_index"]).to_csv(OUT, index=False)
        return

    # Aggregate per team (average across comps/seasons)
    agg = df.groupby("team", as_index=False).agg({
        "psxg_minus_goals_sb": "mean",
        "setpiece_xg_sb": "mean",
        "openplay_xg_sb": "mean"
    }).fillna(0)

    # GK rating: baseline 0.75 plus bonus from positive shot-stopping (scale factor)
    # psxg_minus_goals ~ positive => better keeper
    agg["gk_rating"] = 0.75 + 0.10 * agg["psxg_minus_goals_sb"]
    agg["gk_rating"] = agg["gk_rating"].map(lambda v: clamp(v, 0.55, 0.92))

    # Set-piece rating: fraction of xG from set pieces
    total_xg = agg["setpiece_xg_sb"] + agg["openplay_xg_sb"]
    frac_sp  = np.where(total_xg>0, agg["setpiece_xg_sb"]/total_xg, 0.0)
    agg["setpiece_rating"] = 0.55 + 0.25 * frac_sp
    agg["setpiece_rating"] = agg["setpiece_rating"].map(lambda v: clamp(v, 0.50, 0.90))

    agg["crowd_index"] = 0.70  # hand tune later (e.g., fortress stadiums 0.85–0.95)

    out = agg[["team","gk_rating","setpiece_rating","crowd_index"]].copy()
    out.to_csv(OUT, index=False)
    print(f"[OK] seeded {OUT} rows={len(out)}")

if __name__ == "__main__":
    main()