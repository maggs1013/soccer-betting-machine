#!/usr/bin/env python3
"""
diagnose_connectors_json.py — JSON sidecar of the league-by-league diagnostics

Writes:
  reports/CONNECTOR_DIAG.json
Shape:
{
  "generated": "...Z",
  "lookahead_days": 120,
  "fallback_next": 300,
  "core_ids": ["..."],                       # from CORE_LEAGUE_IDS (if present)
  "rows": [
    {
      "lid": "39",
      "season": 2024,
      "status": 200,                         # last attempted status
      "count": 18,                           # fixtures found
      "used_fallback": true,                 # whether next=N path was required
      "preview": "{...}"                     # short body preview
    },
    ...
  ]
}
"""

import os, sys, csv, json, requests
from datetime import datetime, date, timedelta

REP = "reports"; os.makedirs(REP, exist_ok=True)
OUT = os.path.join(REP, "CONNECTOR_DIAG.json")
AF  = "https://v3.football.api-sports.io"
FD  = "https://api.football-data.org/v4"

def today() -> date: return date.today()
def iso(d: date) -> str: return d.strftime("%Y-%m-%d")
def pv(s: str, n=160) -> str: return (s or "").replace("\n"," ")[:n]

def read_discovery(path="data/discovered_leagues.csv"):
    rows=[]
    if os.path.exists(path):
        try:
            with open(path, newline="", encoding="utf-8") as fh:
                for r in csv.DictReader(fh):
                    lid=(r.get("league_id") or "").strip()
                    sea=(r.get("season") or "").strip()
                    try: sea=int(sea) if sea else None
                    except: sea=None
                    if lid: rows.append({"lid": lid, "season": sea})
        except Exception:
            pass
    return rows

def main():
    key = os.environ.get("API_FOOTBALL_KEY","").strip()
    core = os.environ.get("CORE_LEAGUE_IDS","").strip()
    look = int(os.environ.get("AF_FIXTURES_LOOKAHEAD_DAYS","120"))
    nxt  = int(os.environ.get("AF_FALLBACK_NEXT","300"))
    sample = int(os.environ.get("LEAGUE_CHECK_SAMPLE","10"))

    out = {
        "generated": datetime.utcnow().isoformat()+"Z",
        "lookahead_days": look,
        "fallback_next": nxt,
        "core_ids": [s.strip() for s in core.split(",") if s.strip()],
        "rows": []
    }

    # choose leagues
    cand=[]
    disco = read_discovery()
    if out["core_ids"]:
        # build from core; attach season from discovery if present
        smap = {r["lid"]: r["season"] for r in disco}
        for lid in out["core_ids"]:
            cand.append({"lid": lid, "season": smap.get(lid)})
    else:
        cand = disco[:sample]

    if not key:
        # record error rows for visibility
        for c in cand[:sample]:
            out["rows"].append({"lid": c["lid"], "season": c["season"], "status": None, "count": 0,
                                "used_fallback": False, "preview": "API_FOOTBALL_KEY not set"})
        with open(OUT,"w",encoding="utf-8") as f: json.dump(out,f,ensure_ascii=False,indent=2)
        print(f"diagnose_connectors_json: wrote {OUT}")
        return 0

    hdr = {"x-apisports-key": key}
    t = today(); end = t + timedelta(days=look)

    for c in cand[:sample]:
        lid = c["lid"]; season = c["season"] if c["season"] is not None else t.year
        used_fallback = False
        status = None
        preview = ""
        count = 0

        try:
            # window attempt
            r = requests.get(f"{AF}/fixtures", headers=hdr,
                             params={"league": int(lid), "season": int(season),
                                     "from": iso(t), "to": iso(end)},
                             timeout=45)
            status = r.status_code
            preview = pv(r.text)
            if status == 200:
                try: count = len((r.json() or {}).get("response", []))
                except Exception: count = 0

            # fallback next=N when window empty
            if count == 0:
                r2 = requests.get(f"{AF}/fixtures", headers=hdr,
                                  params={"league": int(lid), "season": int(season), "next": nxt},
                                  timeout=45)
                status = r2.status_code
                preview = pv(r2.text)
                used_fallback = True
                if status == 200:
                    try: count = len((r2.json() or {}).get("response", []))
                    except Exception: count = 0

                # final fallback: next=N without season
                if count == 0:
                    r3 = requests.get(f"{AF}/fixtures", headers=hdr,
                                      params={"league": int(lid), "next": nxt},
                                      timeout=45)
                    status = r3.status_code
                    preview = pv(r3.text)
                    used_fallback = True
                    if status == 200:
                        try: count = len((r3.json() or {}).get("response", []))
                        except Exception: count = 0

        except Exception as e:
            status = None
            preview = pv(str(e))
            count = 0

        out["rows"].append({
            "lid": lid, "season": season, "status": status,
            "count": int(count), "used_fallback": bool(used_fallback),
            "preview": preview
        })

    with open(OUT,"w",encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"diagnose_connectors_json: wrote {OUT}")
    return 0

if __name__=="__main__":
    sys.exit(main())