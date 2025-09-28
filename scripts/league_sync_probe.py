#!/usr/bin/env python3
"""
league_sync_probe.py — verify league coverage alignment

Inputs:
  data/UPCOMING_fixtures.csv
  data/leagues_allowlist.csv     (columns: league, liquidity_tier, max_units)

Output:
  reports/LEAGUE_SYNC.md
- Lists leagues in fixtures but not in allowlist
- Lists allowlisted leagues absent from fixtures
"""

import os, pandas as pd

DATA="data"; REP="reports"
os.makedirs(REP, exist_ok=True)
FIX=os.path.join(DATA,"UPCOMING_fixtures.csv")
ALLOW=os.path.join(DATA,"leagues_allowlist.csv")
OUT=os.path.join(REP,"LEAGUE_SYNC.md")

def safe_read(p):
    if not os.path.exists(p): return pd.DataFrame()
    try: return pd.read_csv(p)
    except Exception: return pd.DataFrame()

def main():
    fx=safe_read(FIX); al=safe_read(ALLOW)
    lines=["# LEAGUE SYNC", ""]
    if fx.empty:
        lines+=["- Fixtures file empty; cannot check sync."]
    else:
        fx_leagues=set(fx["league"].dropna().astype(str)) if "league" in fx.columns else set()
        if al.empty or "league" not in al.columns:
            lines+=["- Allowlist missing or malformed; cannot compare."]
        else:
            al_leagues=set(al["league"].dropna().astype(str))
            extra = sorted(fx_leagues - al_leagues)
            missing = sorted(al_leagues - fx_leagues)
            lines+=["## Comparison", ""]
            lines+=[f"- Leagues in fixtures but not in allowlist: {extra if extra else 'None'}"]
            lines+=[f"- Leagues allowlisted but not in fixtures: {missing if missing else 'None'}"]
    with open(OUT,"w",encoding="utf-8") as f: f.write("\n".join(lines)+"\n")
    print(f"league_sync_probe: wrote {OUT}")

if __name__ == "__main__":
    main()