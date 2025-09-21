# scripts/fetch_understat_xg.py
# Pull Big-5 team xG/xGA from Understat (unofficial) and write team totals.
# Output: data/xg_understat.csv  with columns: team, league, season, xg_u, xga_u

import os
import asyncio
import pandas as pd
from datetime import datetime
from understat import Understat
import aiohttp

DATA = "data"
OUT  = os.path.join(DATA, "xg_understat.csv")

# Understat league codes (Big-5) – season is the starting year, e.g. "2024" for 2024/25
LEAGUES = ["epl", "la_liga", "serie_a", "bundesliga", "ligue_1"]
SEASON  = str(datetime.now().year)  # e.g., 2025 → "2025" (Understat expects "YYYY")

async def fetch():
    os.makedirs(DATA, exist_ok=True)
    rows = []
    async with aiohttp.ClientSession() as session:
        u = Understat(session)
        for lg in LEAGUES:
            try:
                teams = await u.get_teams(lg, season=SEASON)
                for t in teams:
                    name = t.get("title") or t.get("team_title")
                    xg   = float(t.get("xG", 0))
                    xga  = float(t.get("xGA", 0))
                    rows.append({"team": name, "league": lg, "season": SEASON, "xg_u": xg, "xga_u": xga})
                print(f"[OK] Understat: {lg} season {SEASON} teams={len(teams)}")
            except Exception as e:
                print(f"[WARN] Understat fetch failed for {lg}: {e}")
                continue
    pd.DataFrame(rows).to_csv(OUT, index=False)
    print(f"[OK] wrote {OUT} ({len(rows)})")

if __name__ == "__main__":
    asyncio.run(fetch())