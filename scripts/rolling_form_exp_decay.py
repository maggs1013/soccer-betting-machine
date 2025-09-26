#!/usr/bin/env python3
"""
rolling_form_exp_decay.py — exponential-decay form (safer than fixed last5/10)
Inputs:
  data/HIST_matches.csv           (date, league, home_team, away_team, fthg, ftag)
  data/UPCOMING_7D_enriched.csv
Outputs:
  data/UPCOMING_7D_enriched.csv   (updated with exp-decay form)
Adds (per side):
  home_form_xg, home_form_pts, away_form_xg, away_form_pts
Notes:
- We weight the last N matches by lambda^k (k matches ago). Default lambda=0.7, horizon=12.
- Safe: skips if HIST missing; writes enriched unchanged.
"""

import os
import pandas as pd
import numpy as np

DATA="data"
HIST=os.path.join(DATA,"HIST_matches.csv")
UP  = os.path.join(DATA,"UPCOMING_7D_enriched.csv")

LAMBDA= float(os.environ.get("FORM_DECAY_LAMBDA","0.7"))
HORIZ = int(os.environ.get("FORM_DECAY_HORIZON","12"))

REQ = ["date","home_team","away_team","fthg","ftag"]

def safe_read(p):
    if not os.path.exists(p): return pd.DataFrame()
    try: return pd.read_csv(p)
    except Exception: return pd.DataFrame()

def to_pts(hg, ag):
    if pd.isna(hg) or pd.isna(ag): return (np.nan, np.nan)
    if hg>ag: return (3, 0)
    if hg<ag: return (0, 3)
    return (1, 1)

def main():
    up = safe_read(UP)
    hist = safe_read(HIST)
    if up.empty or hist.empty or not set(REQ).issubset(hist.columns):
        up.to_csv(UP, index=False)
        print("rolling_form_exp_decay: missing inputs; wrote enriched unchanged")
        return

    hist["date"]=pd.to_datetime(hist["date"], errors="coerce")
    hist=hist.sort_values("date")

    rows=[]
    for _, r in hist.iterrows():
        h, a = r["home_team"], r["away_team"]
        hg, ag = r["fthg"], r["ftag"]
        pt_h, pt_a = to_pts(hg, ag)
        rows.append({"team":h,"date":r["date"],"xg":hg,"pts":pt_h})
        rows.append({"team":a,"date":r["date"],"xg":ag,"pts":pt_a})
    td = pd.DataFrame(rows).dropna(subset=["team"])

    # compute exp-decay aggregates per team up to most recent historical date
    td = td.sort_values(["team","date"])
    def decay_agg(x):
        x = x.tail(HORIZ)  # limit horizon
        w = np.array([LAMBDA**k for k in range(len(x)-1,-1,-1)], dtype=float)  # older → smaller weight
        w = w / (w.sum() if w.sum()>0 else 1.0)
        return pd.Series({"form_xg": np.nansum(x["xg"].values*w),
                          "form_pts":np.nansum(x["pts"].values*w)})

    latest = td.groupby("team", as_index=False).apply(lambda g: decay_agg(g)).reset_index(level=0, drop=True)
    latest["team"]=td.groupby("team")["team"].tail(1).values

    # map to enriched
    for side in ["home","away"]:
        m = latest.rename(columns={"team":f"{side}_team","form_xg":f"{side}_form_xg","form_pts":f"{side}_form_pts"})
        up = up.merge(m, on=f"{side}_team", how="left")

    up.to_csv(UP, index=False)
    print(f"rolling_form_exp_decay: updated {UP} rows={len(up)} (lambda={LAMBDA}, horizon={HORIZ})")

if __name__ == "__main__":
    main()