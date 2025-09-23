#!/usr/bin/env python3
# Export per-league blend weights and sample counts.
# Reads: data/model_blend.json, data/HIST_matches.csv
# Writes:
#   data/PER_LEAGUE_BLEND_WEIGHTS.csv
#   runs/YYYY-MM-DD/PER_LEAGUE_BLEND_WEIGHTS.csv

import os, json, pandas as pd
from datetime import datetime

DATA = "data"
BLND = os.path.join(DATA, "model_blend.json")
HIST = os.path.join(DATA, "HIST_matches.csv")
OUT_DATA = os.path.join(DATA, "PER_LEAGUE_BLEND_WEIGHTS.csv")
RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
os.makedirs(RUN_DIR, exist_ok=True)
OUT_RUN = os.path.join(RUN_DIR, "PER_LEAGUE_BLEND_WEIGHTS.csv")

def main():
    w_global = 0.85
    w_leagues = {}

    if os.path.exists(BLND):
        try:
            mb = json.load(open(BLND, "r"))
            if isinstance(mb, dict):
                if "w_market" in mb:
                    w_global = float(mb.get("w_market", w_global))
                w_global = float(mb.get("w_market_global", w_global))
                w_leagues = mb.get("w_market_leagues", {}) or {}
        except Exception:
            pass

    if os.path.exists(HIST):
        hist = pd.read_csv(HIST)
        if "league" not in hist.columns:
            hist["league"] = "GLOBAL"
        counts = hist.groupby("league", as_index=False).size().rename(columns={"size":"samples_used"})
    else:
        counts = pd.DataFrame(columns=["league","samples_used"])

    leagues = set(w_leagues.keys()) | set(counts["league"].unique()) if not counts.empty else set(w_leagues.keys())
    if not leagues:
        leagues = {"GLOBAL"}

    stamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    rows = []
    for lg in sorted(leagues):
        w = float(w_leagues.get(lg, w_global))
        n = 0
        if not counts.empty and lg in set(counts["league"]):
            n = int(counts[counts["league"] == lg]["samples_used"].iloc[0])
        rows.append({"league": lg, "w_market": w, "samples_used": n, "last_updated": stamp})

    out = pd.DataFrame(rows)
    out.to_csv(OUT_DATA, index=False)
    out.to_csv(OUT_RUN, index=False)
    print(f"[OK] wrote PER_LEAGUE_BLEND_WEIGHTS to {OUT_DATA} and {OUT_RUN} (rows={len(out)})")

if __name__ == "__main__":
    main()