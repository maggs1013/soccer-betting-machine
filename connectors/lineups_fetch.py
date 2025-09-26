#!/usr/bin/env python3
"""
lineups_fetch.py — actual starters + experienced starters %
Requires:
  data/UPCOMING_fixtures.csv with api_football_fixture_id and api_football_home_id/away_id
Env:
  API_FOOTBALL_KEY (x-apisports-key)
Writes:
  data/lineups.csv with:
    fixture_id,
    home_avail, away_avail (starters/11),
    home_injury_index, away_injury_index (1 - avail),
    home_exp_starters_pct, away_exp_starters_pct (starters with >=900 mins this season / 11)

Safe: if any API call fails, writes NaN for that metric.
"""

import os, math, requests
import pandas as pd
from datetime import datetime

DATA = "data"
FIX = os.path.join(DATA, "UPCOMING_fixtures.csv")
OUT = os.path.join(DATA, "lineups.csv")

def safe_read(path): 
    if not os.path.exists(path): return pd.DataFrame()
    try: return pd.read_csv(path)
    except Exception: return pd.DataFrame()

def normalize_fixture_id(row):
    d = str(row.get("date","NA")).replace("-","").replace("T","_").replace(":","")
    h = str(row.get("home_team","NA")).strip().lower().replace(" ","_")
    a = str(row.get("away_team","NA")).strip().lower().replace(" ","_")
    return f"{d}__{h}__vs__{a}"

def af_players_minutes(team_id: int, season: int, key: str) -> dict:
    """Return dict player_name_lower -> season minutes (>=0)."""
    try:
        r = requests.get("https://v3.football.api-sports.io/players",
                         headers={"x-apisports-key": key},
                         params={"team": int(team_id), "season": int(season)},
                         timeout=40)
        if r.status_code != 200: return {}
        mins = {}
        for item in (r.json() or {}).get("response", []):
            player = (item.get("player") or {}).get("name") or ""
            stats = item.get("statistics") or []
            total_mins = 0
            for st in stats:
                mt = ((st.get("games") or {}).get("minutes") or 0) or 0
                total_mins += mt
            if player:
                mins[player.strip().lower()] = total_mins
        return mins
    except Exception:
        return {}

def main():
    key = os.environ.get("API_FOOTBALL_KEY","").strip()
    fx = safe_read(FIX)
    if fx.empty:
        pd.DataFrame(columns=[
            "fixture_id","home_injury_index","away_injury_index",
            "home_avail","away_avail","home_exp_starters_pct","away_exp_starters_pct"
        ]).to_csv(OUT, index=False)
        print("lineups_fetch: no fixtures; wrote header-only")
        return

    if "fixture_id" not in fx.columns:
        fx["fixture_id"] = fx.apply(normalize_fixture_id, axis=1)

    rows = []
    for _, r in fx.iterrows():
        my_fix = r.get("fixture_id")
        af_id = r.get("api_football_fixture_id")
        home_id = r.get("api_football_home_id")
        away_id = r.get("api_football_away_id")
        season = r.get("api_football_season")

        home_av = away_av = float("nan")
        home_inj = away_inj = float("nan")
        home_exp = away_exp = float("nan")

        if not key or pd.isna(af_id):
            rows.append({
                "fixture_id": my_fix,
                "home_injury_index": home_inj, "away_injury_index": away_inj,
                "home_avail": home_av, "away_avail": away_av,
                "home_exp_starters_pct": home_exp, "away_exp_starters_pct": away_exp
            })
            continue

        # 1) fixture lineups
        try:
            lr = requests.get("https://v3.football.api-sports.io/fixtures/lineups",
                              headers={"x-apisports-key": key},
                              params={"fixture": int(af_id)}, timeout=40)
            if lr.status_code == 200:
                resp = (lr.json() or {}).get("response", [])
                starters_map = {}  # team_name_lower -> set of starter names lower
                for side in resp:
                    tname = (side.get("team") or {}).get("name") or ""
                    tkey = tname.strip().lower()
                    startXI = side.get("startXI") or []
                    sn = set()
                    for s in startXI:
                        pl = (s.get("player") or {}).get("name") or ""
                        if pl: sn.add(pl.strip().lower())
                    starters_map[tkey] = sn
                # map to our home/away by partial match against our names
                hkey = str(r.get("home_team","")).strip().lower()
                akey = str(r.get("away_team","")).strip().lower()
                hstar = starters_map.get(hkey, set())
                if not hstar:
                    for k in starters_map:
                        if k and k in hkey: hstar = starters_map[k]; break
                astar = starters_map.get(akey, set())
                if not astar:
                    for k in starters_map:
                        if k and k in akey: astar = starters_map[k]; break
                home_av = (len(hstar)/11.0) if hstar else float("nan")
                away_av = (len(astar)/11.0) if astar else float("nan")
                if not math.isnan(home_av): home_inj = 1.0 - home_av
                if not math.isnan(away_av): away_inj = 1.0 - away_av
            else:
                pass
        except Exception:
            pass

        # 2) experienced starters % using season minutes (>=900 min = experienced)
        try:
            if pd.notna(home_id) and pd.notna(season):
                hmins = af_players_minutes(int(home_id), int(season), key)
                if isinstance(hmins, dict) and home_av == home_av:
                    # proportion of starters with minutes >=900
                    count = sum(1 for n in (hstar if 'hstar' in locals() else []) if hmins.get(n, 0) >= 900)
                    home_exp = (count / 11.0) if (hstar if 'hstar' in locals() else None) else float("nan")
            if pd.notna(away_id) and pd.notna(season):
                amins = af_players_minutes(int(away_id), int(season), key)
                if isinstance(amins, dict) and away_av == away_av:
                    count = sum(1 for n in (astar if 'astar' in locals() else []) if amins.get(n, 0) >= 900)
                    away_exp = (count / 11.0) if (astar if 'astar' in locals() else None) else float("nan")
        except Exception:
            pass

        rows.append({
            "fixture_id": my_fix,
            "home_injury_index": home_inj, "away_injury_index": away_inj,
            "home_avail": home_av, "away_avail": away_av,
            "home_exp_starters_pct": home_exp, "away_exp_starters_pct": away_exp
        })

    pd.DataFrame(rows).to_csv(OUT, index=False)
    print(f"lineups_fetch: wrote {OUT} rows={len(rows)}")

if __name__ == "__main__":
    main()