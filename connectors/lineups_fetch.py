#!/usr/bin/env python3
"""
lineups_fetch.py — API-FOOTBALL lineups (actual starters) with fixture IDs
- Requires: data/UPCOMING_fixtures.csv with api_football_fixture_id
- Env: API_FOOTBALL_KEY (x-apisports-key)
- Writes: data/lineups.csv with fixture-level availability & injury indices

Columns:
  fixture_id                (your normalized fixture key; stable)
  home_avail, away_avail    (0..1 starters present/11)
  home_injury_index         (1 - home_avail)
  away_injury_index         (1 - away_avail)
"""

import os, math, requests
import pandas as pd
from datetime import datetime, timezone

DATA = "data"
FIX = os.path.join(DATA, "UPCOMING_fixtures.csv")
OUT = os.path.join(DATA, "lineups.csv")

def to_utc_dt(iso_str):
    try: return datetime.fromisoformat(iso_str.replace("Z","+00:00")).astimezone(timezone.utc)
    except Exception: return None

def normalize_fixture_id(row):
    # same as enrich_features.py
    d = str(row.get("date","NA")).replace("-","").replace("T","_").replace(":","")
    h = str(row.get("home_team","NA")).strip().lower().replace(" ","_")
    a = str(row.get("away_team","NA")).strip().lower().replace(" ","_")
    return f"{d}__{h}__vs__{a}"

def safe_read(path):
    if not os.path.exists(path): return pd.DataFrame()
    try: return pd.read_csv(path)
    except Exception: return pd.DataFrame()

def main():
    key = os.environ.get("API_FOOTBALL_KEY","").strip()
    fx = safe_read(FIX)
    if fx.empty:
        pd.DataFrame(columns=["fixture_id","home_injury_index","away_injury_index","home_avail","away_avail"]).to_csv(OUT, index=False)
        print("lineups_fetch: no fixtures; wrote header-only")
        return

    if "fixture_id" not in fx.columns:
        fx["fixture_id"] = fx.apply(normalize_fixture_id, axis=1)

    if not key:
        # no provider key: output safe header, enrichment will default
        pd.DataFrame(columns=["fixture_id","home_injury_index","away_injury_index","home_avail","away_avail"]).to_csv(OUT, index=False)
        print("lineups_fetch: API_FOOTBALL_KEY missing; wrote header-only")
        return

    headers = {"x-apisports-key": key}
    out_rows = []

    for _, r in fx.iterrows():
        af_fix_id = r.get("api_football_fixture_id")
        my_fix_id = r.get("fixture_id")
        if pd.isna(af_fix_id):
            # no official id match -> skip gracefully
            out_rows.append({"fixture_id": my_fix_id,
                             "home_injury_index": float("nan"),
                             "away_injury_index": float("nan"),
                             "home_avail": float("nan"),
                             "away_avail": float("nan")})
            continue

        try:
            resp = requests.get("https://v3.football.api-sports.io/fixtures/lineups",
                                headers=headers, params={"fixture": int(af_fix_id)}, timeout=40)
            if resp.status_code != 200:
                out_rows.append({"fixture_id": my_fix_id,
                                 "home_injury_index": float("nan"),
                                 "away_injury_index": float("nan"),
                                 "home_avail": float("nan"),
                                 "away_avail": float("nan")})
                continue
            data = (resp.json() or {}).get("response", [])
            # API returns up to 2 rows: home, away
            home_avail = away_avail = float("nan")
            for side in data:
                team = (side.get("team") or {})
                start11 = side.get("startXI") or []
                starters = len(start11)
                avail = starters/11.0 if starters and starters > 0 else float("nan")
                # Heuristic: first entry should be home if sorted, but safer to match by name
                # We compare to our fixture teams (best-effort)
                team_name = (team.get("name") or "").strip().lower()
                hname = str(r.get("home_team","")).strip().lower()
                aname = str(r.get("away_team","")).strip().lower()
                if team_name and hname and team_name in hname:
                    home_avail = avail
                elif team_name and aname and team_name in aname:
                    away_avail = avail

            row = {"fixture_id": my_fix_id,
                   "home_avail": home_avail, "away_avail": away_avail,
                   "home_injury_index": (1.0 - home_avail) if not math.isnan(home_avail) else float("nan"),
                   "away_injury_index": (1.0 - away_avail) if not math.isnan(away_avail) else float("nan")}
            out_rows.append(row)
        except Exception:
            out_rows.append({"fixture_id": my_fix_id,
                             "home_injury_index": float("nan"),
                             "away_injury_index": float("nan"),
                             "home_avail": float("nan"),
                             "away_avail": float("nan")})

    pd.DataFrame(out_rows).to_csv(OUT, index=False)
    print(f"lineups_fetch: wrote {OUT} rows={len(out_rows)}")

if __name__ == "__main__":
    main()