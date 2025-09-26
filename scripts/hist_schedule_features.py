#!/usr/bin/env python3
"""
hist_schedule_features.py — schedule micro-features from HIST
Inputs:
  data/HIST_matches.csv        (date, home_team, away_team, fthg, ftag)
  data/UPCOMING_7D_enriched.csv
Outputs:
  data/UPCOMING_7D_enriched.csv  (adds: rest_days_home/away, games_in_last7/14_home/away)
"""

import os, pandas as pd, numpy as np

DATA="data"
HIST=os.path.join(DATA,"HIST_matches.csv")
UP  = os.path.join(DATA,"UPCOMING_7D_enriched.csv")

def safe_read(p):
    if not os.path.exists(p): return pd.DataFrame()
    try: return pd.read_csv(p)
    except Exception: return pd.DataFrame()

def main():
    up = safe_read(UP)
    hist = safe_read(HIST)
    if up.empty or hist.empty or "date" not in hist.columns:
        up.to_csv(UP, index=False); print("hist_schedule_features: missing inputs; wrote unchanged"); return

    hist["date"]=pd.to_datetime(hist["date"], errors="coerce")
    # Build per-team last match date list
    events=[]
    for _, r in hist.iterrows():
        events.append({"team": r["home_team"], "date": r["date"]})
        events.append({"team": r["away_team"], "date": r["date"]})
    td=pd.DataFrame(events).dropna()
    td=td.sort_values(["team","date"])

    # Precompute rolling counts
    def count_window(g, d, days):
        return ((g["date"]<=d) & (g["date"]>d-pd.Timedelta(days=days))).sum()

    # Attach per-fixture schedule feats
    if "date" in up.columns:
        up["date"]=pd.to_datetime(up["date"], errors="coerce")
        rest_h=[]; rest_a=[]; g7_h=[]; g7_a=[]; g14_h=[]; g14_a=[]
        for _, r in up.iterrows():
            d=r.get("date"); h=r.get("home_team"); a=r.get("away_team")
            if pd.isna(d) or not isinstance(d, pd.Timestamp):
                rest_h.append(np.nan); rest_a.append(np.nan); g7_h.append(np.nan); g7_a.append(np.nan); g14_h.append(np.nan); g14_a.append(np.nan); continue
            gh=td[td["team"]==h]; ga=td[td["team"]==a]
            last_h=gh[gh["date"]<d]["date"].max() if not gh.empty else pd.NaT
            last_a=ga[ga["date"]<d]["date"].max() if not ga.empty else pd.NaT
            rest_h.append((d-last_h).days if isinstance(last_h, pd.Timestamp) else np.nan)
            rest_a.append((d-last_a).days if isinstance(last_a, pd.Timestamp) else np.nan)
            g7_h.append(count_window(gh, d, 7));  g7_a.append(count_window(ga, d, 7))
            g14_h.append(count_window(gh, d, 14)); g14_a.append(count_window(ga, d, 14))
        up["home_rest_days"]=rest_h; up["away_rest_days"]=rest_a
        up["home_games_last7"]=g7_h; up["away_games_last7"]=g7_a
        up["home_games_last14"]=g14_h; up["away_games_last14"]=g14_a

    up.to_csv(UP, index=False)
    print(f"hist_schedule_features: updated {UP} rows={len(up)}")

if __name__ == "__main__":
    main()