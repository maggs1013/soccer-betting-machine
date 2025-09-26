#!/usr/bin/env python3
"""
Fetch lineups from API-FOOTBALL (no manual input)
- Uses fixtures in data/UPCOMING_fixtures.csv (home_team/away_team/date)
- Queries /fixtures/lineups for each fixture's approximate match (by date+teams)
- Writes: data/lineups.csv   (one row per team per fixture; starting XI + formation)
NOTE: Requires secret: API_FOOTBALL_KEY
"""

import os, csv, requests
from datetime import datetime, timedelta, timezone

DATA_DIR = "data"
IN_FIX = os.path.join(DATA_DIR, "UPCOMING_fixtures.csv")
OUT = os.path.join(DATA_DIR, "lineups.csv")

API = "https://v3.football.api-sports.io"
KEY = os.environ.get("API_FOOTBALL_KEY","").strip()

def parse_iso(s):
    try:
        return datetime.fromisoformat(s.replace("Z","+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def main():
    if not KEY:
        # write empty with header to avoid crash
        with open(OUT,"w",encoding="utf-8") as f:
            f.write("date,home_team,away_team,side,formation,coach,players\n")
        print("API_FOOTBALL_KEY missing; wrote empty lineups.csv")
        return

    if not os.path.exists(IN_FIX):
        with open(OUT,"w",encoding="utf-8") as f:
            f.write("date,home_team,away_team,side,formation,coach,players\n")
        print("UPCOMING_fixtures.csv missing; wrote empty lineups.csv")
        return

    rows_out = []
    with open(IN_FIX,"r",encoding="utf-8") as f:
        header = f.readline().strip().split(",")
        cols = {name:i for i,name in enumerate(header)}
        for line in f:
            parts = line.strip().split(",")
            if len(parts) < len(header): continue
            date = parts[cols["date"]]
            ht = parts[cols["home_team"]]
            at = parts[cols["away_team"]]
            ko = parse_iso(date)
            if not ko: continue

            # Find fixture by date proximity ±1day and team names
            # First, search fixtures by date window and team names (API-FOOTBALL typically uses IDs; here we do a simple match)
            # More robust: you may cache mapping from previous runs (fixture_id).
            params = {"date": ko.strftime("%Y-%m-%d")}
            resp = requests.get(f"{API}/fixtures", headers={"x-apisports-key": KEY}, params=params, timeout=25)
            if resp.status_code != 200: continue
            cand = (resp.json() or {}).get("response") or []
            fx_id = None
            for m in cand:
                th = (m.get("teams",{}).get("home",{}) or {}).get("name","").strip().lower()
                ta = (m.get("teams",{}).get("away",{}) or {}).get("name","").strip().lower()
                if th == ht.strip().lower() and ta == at.strip().lower():
                    fx_id = (m.get("fixture") or {}).get("id")
                    break
                # try reversed names just in case
                if th == at.strip().lower() and ta == ht.strip().lower():
                    fx_id = (m.get("fixture") or {}).get("id")
                    break
            if not fx_id:  # skip if not found
                continue

            # Now fetch lineups for fixture id
            lu = requests.get(f"{API}/fixtures/lineups", headers={"x-apisports-key": KEY}, params={"fixture": fx_id}, timeout=25)
            if lu.status_code != 200: continue
            items = (lu.json() or {}).get("response") or []
            for side in items:
                team_name = (side.get("team") or {}).get("name","")
                side_tag = "home" if team_name.strip().lower()==ht.strip().lower() else ("away" if team_name.strip().lower()==at.strip().lower() else "unknown")
                form = side.get("formation") or ""
                coach = (side.get("coach") or {}).get("name","")
                players = []
                for p in side.get("startXI") or []:
                    pl = p.get("player") or {}
                    num = pl.get("number")
                    nm = pl.get("name")
                    pos = pl.get("pos")
                    players.append(f"{num}:{nm}:{pos}")
                rows_out.append({
                    "date": date, "home_team": ht, "away_team": at,
                    "side": side_tag, "formation": form, "coach": coach,
                    "players": "|".join(players)
                })

    os.makedirs(DATA_DIR, exist_ok=True)
    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["date","home_team","away_team","side","formation","coach","players"])
        w.writeheader(); w.writerows(rows_out)

    print(f"✅ Wrote {len(rows_out)} lineup rows → {OUT}")

if __name__ == "__main__":
    main()