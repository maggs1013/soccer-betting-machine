#!/usr/bin/env python3
"""
lineups_fetch_apifootball.py
- Reads data/UPCOMING_fixtures.csv (must contain api_football_fixture_id)
- Calls /fixtures/lineups for each fixture
- Computes:
    home_starters, away_starters (count)
    home_avail, away_avail (starters/11, clipped [0,1])
    home_injury_index = 1 - home_avail
    away_injury_index = 1 - away_avail
- Writes data/lineups.csv with these columns keyed by fixture_id
Notes:
- Requires env API_FOOTBALL_KEY
- Safe: if no key or no ids found, writes header-only file
"""

import os, requests, time
import pandas as pd

DATA = "data"
FIX = os.path.join(DATA, "UPCOMING_fixtures.csv")
OUT = os.path.join(DATA, "lineups.csv")

def safe_read(path):
    if not os.path.exists(path): return pd.DataFrame()
    try: return pd.read_csv(path)
    except Exception: return pd.DataFrame()

def main():
    key = os.environ.get("API_FOOTBALL_KEY","").strip()
    fx = safe_read(FIX)
    cols = ["fixture_id","home_starters","away_starters","home_avail","away_avail","home_injury_index","away_injury_index"]
    if fx.empty or "fixture_id" not in fx.columns:
        pd.DataFrame(columns=cols).to_csv(OUT, index=False); print("lineups: no fixtures"); return

    if not key:
        pd.DataFrame(columns=cols).to_csv(OUT, index=False); print("lineups: API_FOOTBALL_KEY missing"); return

    if "api_football_fixture_id" not in fx.columns:
        pd.DataFrame(columns=cols).to_csv(OUT, index=False); print("lineups: no api_football_fixture_id; run augment first"); return

    headers = {"x-apisports-key": key}
    out_rows = []

    for _, r in fx.iterrows():
        fid = r.get("api_football_fixture_id")
        if pd.isna(fid):  # no mapped id
            continue
        try:
            resp = requests.get("https://v3.football.api-sports.io/fixtures/lineups",
                                headers=headers, params={"fixture": int(fid)}, timeout=30)
            if resp.status_code != 200:
                continue
            arr = (resp.json() or {}).get("response") or []
            # Response typically two entries: home, away
            home_starters = away_starters = 0
            for item in arr:
                team_side = item.get("team", {}).get("name","")
                sx = item.get("startXI") or []
                starters = sum(1 for _ in sx)
                # crude side infer: match by normalized team name to fixture
                # safer: compare to home/away names if you store provider names; here we assign first/second
                if home_starters == 0:
                    home_starters = starters
                else:
                    away_starters = starters
            # clip & compute
            h_av = max(0.0, min(1.0, (home_starters or 0)/11.0))
            a_av = max(0.0, min(1.0, (away_starters or 0)/11.0))
            out_rows.append({
                "fixture_id": r["fixture_id"],
                "home_starters": home_starters or 0,
                "away_starters": away_starters or 0,
                "home_avail": h_av,
                "away_avail": a_av,
                "home_injury_index": 1.0 - h_av,
                "away_injury_index": 1.0 - a_av
            })
            time.sleep(0.35)
        except Exception:
            continue

    pd.DataFrame(out_rows, columns=cols).to_csv(OUT, index=False)
    print(f"lineups: wrote {OUT} rows={len(out_rows)}")

if __name__ == "__main__":
    main()