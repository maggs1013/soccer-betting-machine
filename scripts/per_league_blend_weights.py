# scripts/per_league_blend_weights.py
# Export per-league blend weights and sample counts.
# Reads: data/model_blend.json, data/HIST_matches.csv
# Writes: data/PER_LEAGUE_BLEND_WEIGHTS.csv

import os, json, pandas as pd
from datetime import datetime

DATA = "data"
BLND = os.path.join(DATA, "model_blend.json")
HIST = os.path.join(DATA, "HIST_matches.csv")
OUT  = os.path.join(DATA, "PER_LEAGUE_BLEND_WEIGHTS.csv")

def main():
    if not os.path.exists(BLND):
        pd.DataFrame(columns=["league","w_market","samples_used","last_updated"]).to_csv(OUT,index=False)
        print(f"[WARN] {BLND} missing; wrote empty {OUT}")
        return

    mb = json.load(open(BLND,"r"))
    w_global = float(mb.get("w_market_global", 0.85))
    w_leagues = mb.get("w_market_leagues", {}) or {}

    if os.path.exists(HIST):
        hist = pd.read_csv(HIST)
        if "league" not in hist.columns: hist["league"] = "GLOBAL"
        counts = hist.groupby("league", as_index=False).size().rename(columns={"size":"samples_used"})
    else:
        counts = pd.DataFrame(columns=["league","samples_used"])

    rows = []
    leags = set(list(w_leagues.keys()) + list(counts["league"].unique() if not counts.empty else []))
    if not leags:
        leags = {"GLOBAL"}

    stamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    for lg in sorted(leags):
        w = float(w_leagues.get(lg, w_global))
        n = int(counts[counts["league"]==lg]["samples_used"].iloc[0]) if lg in set(counts["league"]) else 0
        rows.append({"league": lg, "w_market": w, "samples_used": n, "last_updated": stamp})

    pd.DataFrame(rows).to_csv(OUT, index=False)
    print(f"[OK] wrote {OUT} rows={len(rows)}")

if __name__ == "__main__":
    main()