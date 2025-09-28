#!/usr/bin/env python3
"""
fixtures_debug_probe.py — pinpoint why 'no upcoming games' appears

Outputs:
  reports/FIXTURES_DEBUG.md

It will:
- Load data/UPCOMING_fixtures.csv
- Show row count, sample rows, date parse success (%), min/max kickoff
- List distinct leagues and counts
- Report % missing for critical columns: date, home_team, away_team, league, fixture_id
- Warn if nothing falls in [now, now+7d] after UTC parse
"""

import os, pandas as pd, numpy as np
from datetime import datetime, timezone, timedelta

DATA="data"; REP="reports"
os.makedirs(REP, exist_ok=True)
FIX=os.path.join(DATA,"UPCOMING_fixtures.csv")
OUT=os.path.join(REP,"FIXTURES_DEBUG.md")

def safe_read(p):
    if not os.path.exists(p): return pd.DataFrame()
    try: return pd.read_csv(p)
    except Exception: return pd.DataFrame()

def ensure_fixture_id(df):
    if df.empty: return df
    if "fixture_id" in df.columns: return df
    if {"date","home_team","away_team"}.issubset(df.columns):
        def mk_id(r):
            d=str(r.get("date","NA")).replace("-","").replace("T","_").replace(":","")
            h=str(r.get("home_team","NA")).strip().lower().replace(" ","_")
            a=str(r.get("away_team","NA")).strip().lower().replace(" ","_")
            return f"{d}__{h}__vs__{a}"
        df=df.copy()
        df["fixture_id"]=df.apply(mk_id, axis=1)
    return df

def main():
    now = datetime.now(timezone.utc)
    end = now + timedelta(days=7)
    fx = safe_read(FIX)
    lines = ["# FIXTURES DEBUG", f"_Generated: {now.isoformat()}_", ""]

    if fx.empty:
        lines += ["- `UPCOMING_fixtures.csv` has **0 rows**."]
        with open(OUT,"w",encoding="utf-8") as f: f.write("\n".join(lines)+"\n")
        print("fixtures_debug_probe: fixtures empty"); return

    # basic stats
    lines += [f"- rows: **{len(fx)}**", ""]
    head = fx.head(10).to_string(index=False)
    lines += ["## Sample rows (head 10)", "", "```\n"+head+"\n```", ""]

    # missing %
    need = ["date","home_team","away_team","league","fixture_id"]
    fx = ensure_fixture_id(fx)
    miss = []
    for c in need:
        if c in fx.columns:
            miss.append((c, fx[c].isna().mean()*100.0))
        else:
            miss.append((c, 100.0))
    lines += ["## Missing % for required fields", "", "| Field | % Missing |", "|---|---:|"]
    for c,p in miss: lines.append(f"| {c} | {p:.0f}% |")
    lines.append("")

    # date parse & window
    if "date" in fx.columns:
        dt = pd.to_datetime(fx["date"], errors="coerce", utc=True)
        fx["_dt"] = dt
        lines += [f"- Date parse success: **{dt.notna().mean()*100:.0f}%**"]
        if dt.notna().any():
            lines += [f"- kickoff min: **{dt.min()}**, max: **{dt.max()}**"]
        inwin = dt.notna() & (dt>=now) & (dt<=end)
        lines += [f"- rows in [now, +7d]: **{int(inwin.sum())}**", ""]
    else:
        lines += ["- No `date` column present.", ""]

    # leagues
    if "league" in fx.columns:
        cnt = fx.groupby("league").size().reset_index(name="n").sort_values("n",ascending=False)
        lines += ["## Leagues present", "", cnt.to_string(index=False), ""]
    else:
        lines += ["- No `league` column present.", ""]

    with open(OUT,"w",encoding="utf-8") as f:
        f.write("\n".join(lines)+"\n")
    print(f"fixtures_debug_probe: wrote {OUT}")

if __name__ == "__main__":
    main()