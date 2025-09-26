#!/usr/bin/env python3
"""
ref_cards_corners_build.py — seasonal priors from Football-Data.org
Inputs:
  data/HIST_matches.csv   (must contain: date, league, home_team, away_team,
                           fthg, ftag, HC, AC, HR, AR, referee if present)
Outputs:
  data/ref_cards_corners.csv  (season averages by team/ref/league)
"""

import os, pandas as pd

DATA="data"
HIST=os.path.join(DATA,"HIST_matches.csv")
OUT=os.path.join(DATA,"ref_cards_corners.csv")

def safe_read(p):
    if not os.path.exists(p): return pd.DataFrame()
    try: return pd.read_csv(p)
    except Exception: return pd.DataFrame()

def main():
    hist=safe_read(HIST)
    if hist.empty:
        pd.DataFrame(columns=["league","team","season","avg_cards","avg_corners"]).to_csv(OUT,index=False)
        print("ref_cards_corners_build: no hist; wrote header-only")
        return

    # try to infer season from date
    hist["date"]=pd.to_datetime(hist["date"],errors="coerce")
    hist["season"]=hist["date"].dt.year
    rows=[]

    for _,r in hist.iterrows():
        season=r["season"]; lg=r.get("league"); ref=r.get("referee",None)
        # Home row
        rows.append({"league":lg,"team":r["home_team"],"season":season,
                     "cards":(r.get("HR",0) or 0),
                     "corners":(r.get("HC",0) or 0),"referee":ref})
        # Away row
        rows.append({"league":lg,"team":r["away_team"],"season":season,
                     "cards":(r.get("AR",0) or 0),
                     "corners":(r.get("AC",0) or 0),"referee":ref})

    df=pd.DataFrame(rows)
    priors=df.groupby(["league","team","season"],as_index=False).agg(
        avg_cards=("cards","mean"),avg_corners=("corners","mean"))
    # referee priors if column present
    refpri=pd.DataFrame()
    if "referee" in df.columns:
        refpri=df.groupby(["referee","season"],as_index=False).agg(
            ref_avg_cards=("cards","mean"))
    out=priors.merge(refpri,on="season",how="left") if not refpri.empty else priors
    out.to_csv(OUT,index=False)
    print(f"ref_cards_corners_build: wrote {OUT} rows={len(out)}")

if __name__=="__main__":
    main()