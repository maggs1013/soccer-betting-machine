#!/usr/bin/env python3
"""
discovery_seasons_export.py — JSON sidecar for discovery seasons

Reads:
  data/discovered_leagues.csv

Writes:
  reports/DISCOVERY_SEASONS.json
Shape:
{
  "generated":"...Z",
  "rows": N,
  "seasons": {"39": 2024, "140": 2024, ...},
  "sample": [
     {"league_id":"39","league_name":"Premier League","country":"England","season":2024,"type":"League"},
     ...
  ]
}
"""

import os, csv, json
from datetime import datetime

REP = "reports"; os.makedirs(REP, exist_ok=True)
OUT = os.path.join(REP, "DISCOVERY_SEASONS.json")
SRC = "data/discovered_leagues.csv"

def main():
    data = {"generated": datetime.utcnow().isoformat()+"Z", "rows": 0, "seasons": {}, "sample": []}
    if os.path.exists(SRC):
        with open(SRC, newline="", encoding="utf-8") as fh:
            rdr = csv.DictReader(fh)
            rows = list(rdr)
            data["rows"] = len(rows)
            for i, r in enumerate(rows):
                lid = (r.get("league_id") or "").strip()
                sn  = (r.get("season") or "").strip()
                if lid and sn.isdigit():
                    data["seasons"][lid] = int(sn)
                if i < 15:
                    data["sample"].append({
                        "league_id": lid,
                        "league_name": (r.get("league_name") or "").strip(),
                        "country": (r.get("country") or "").strip(),
                        "season": int(sn) if sn.isdigit() else None,
                        "type": (r.get("type") or "").strip()
                    })
    with open(OUT,"w",encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"discovery_seasons_export: wrote {OUT}")
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())