#!/usr/bin/env python3
"""
diagnose_connectors.py — league-by-league forensic probe

Why this script?
----------------
When the smoke job still shows zeros, this prints hard evidence so we can
pinpoint the cause in one pass:

- Secret presence (masked: SET / NOT SET)
- Discovery outcome:
    * how many rows were discovered
    * first 15 rows from data/discovered_leagues.csv
- API-Football per-league probe (first N leagues):
    * uses the per-league 'season' from discovery (NOT calendar year)
    * calls /fixtures with [today, today + AF_FIXTURES_LOOKAHEAD_DAYS]
    * prints a table: league_id, season, HTTP status, count, short preview
- Football-Data.org checks (tokened if available):
    * /competitions status + preview
    * /competitions/PL/matches status + preview

Writes:
  reports/CONNECTOR_DIAG.md

Env knobs (safe defaults):
  API_FOOTBALL_KEY        : required for API-Football probe
  FDORG_TOKEN             : optional (recommended to avoid FD.org quotas)
  LEAGUE_CHECK_SAMPLE     : how many leagues to probe (default "10")
  AF_FIXTURES_LOOKAHEAD_DAYS : forward window in days (default "120")
  HTTP_TIMEOUT_SEC / HTTP_RETRIES : not used here (simple direct GETs)
"""

import os
import sys
import csv
import json
import requests
from datetime import datetime, date, timedelta

REP = "reports"
os.makedirs(REP, exist_ok=True)
OUT = os.path.join(REP, "CONNECTOR_DIAG.md")

AF_BASE = "https://v3.football.api-sports.io"
FD_BASE = "https://api.football-data.org/v4"

def _today() -> date:
    return date.today()

def _iso(d: date) -> str:
    return d.strftime("%Y-%m-%d")

def _read_discovery(path: str = "data/discovered_leagues.csv"):
    """
    Returns a list of dicts:
      {league_id:str, league_name:str, country:str, season:int|None, type:str}
    """
    rows = []
    if not os.path.exists(path):
        return rows
    try:
        with open(path, newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                lid = (r.get("league_id") or "").strip()
                nm  = (r.get("league_name") or "").strip()
                cn  = (r.get("country") or "").strip()
                tp  = (r.get("type") or "").strip()
                s_raw = (r.get("season") or "").strip()
                try:
                    ssn = int(s_raw) if s_raw else None
                except Exception:
                    ssn = None
                if lid:
                    rows.append({
                        "league_id": lid,
                        "league_name": nm,
                        "country": cn,
                        "season": ssn,
                        "type": tp
                    })
    except Exception:
        pass
    return rows

def _preview(s: str, n: int = 120) -> str:
    s = s or ""
    s = s.replace("\n", " ")
    return s[:n]

def _md_write(lines):
    with open(OUT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"diagnose_connectors: wrote {OUT}")

def main():
    lines = ["# CONNECTOR DIAG", f"_Generated: {datetime.utcnow().isoformat()}Z_", ""]

    # ----- presence checks (masked) -----
    api_football_key = os.environ.get("API_FOOTBALL_KEY", "").strip()
    fdorg_token      = os.environ.get("FDORG_TOKEN", "").strip()
    lines += ["## Secret presence", ""]
    lines += [f"- API_FOOTBALL_KEY: {'SET' if api_football_key else 'NOT SET'}"]
    lines += [f"- FDORG_TOKEN: {'SET' if fdorg_token else 'NOT SET'}", ""]

    # ----- discovery outcome -----
    disc = _read_discovery()
    lines += [f"## Discovery summary", f"- discovered rows: **{len(disc)}**", ""]
    if disc:
        lines += ["| league_id | season | league_name | country | type |", "|---:|---:|---|---|---|"]
        for r in disc[:15]:
            lines.append(f"| {r['league_id']} | {r['season'] if r['season'] is not None else ''} "
                         f"| {r['league_name']} | {r['country']} | {r['type']} |")
        lines.append("")
    else:
        lines += ["- (no discovered_leagues.csv rows found)", ""]

    # ----- API-Football per-league fixtures probe -----
    lines += ["## API-Football per-league fixtures probe", ""]
    if not api_football_key:
        lines += ["- Skipped: API_FOOTBALL_KEY not set", ""]
    else:
        headers = {"x-apisports-key": api_football_key}
        try:
            sample_n = int(os.environ.get("LEAGUE_CHECK_SAMPLE", "10"))
        except Exception:
            sample_n = 10
        try:
            lookahead = int(os.environ.get("AF_FIXTURES_LOOKAHEAD_DAYS", "120"))
        except Exception:
            lookahead = 120

        t = _today()
        end = t + timedelta(days=lookahead)
        lines += ["| lid | season | status | count | preview |", "|---:|---:|---:|---:|---|"]

        for r in disc[:sample_n]:
            lid = r["league_id"]
            season = r["season"] if r["season"] is not None else t.year
            try:
                resp = requests.get(f"{AF_BASE}/fixtures",
                                    headers=headers,
                                    params={"league": int(lid), "season": int(season),
                                            "from": _iso(t), "to": _iso(end)},
                                    timeout=45)
                status = resp.status_code
                pv = _preview(resp.text)
                cnt = 0
                if status == 200:
                    try:
                        cnt = len((resp.json() or {}).get("response", []))
                    except Exception:
                        cnt = 0
                lines.append(f"| {lid} | {season} | {status} | {cnt} | `{pv}` |")
            except Exception as e:
                lines.append(f"| {lid} | {season} | ERR | 0 | `{_preview(str(e))}` |")
        lines.append("")

    # ----- Football-Data.org checks -----
    lines += ["## Football-Data.org checks", ""]
    hdr = {"X-Auth-Token": fdorg_token} if fdorg_token else {}
    # /competitions
    try:
        r = requests.get(f"{FD_BASE}/competitions", headers=hdr, timeout=40)
        lines += [f"- /competitions status: **{r.status_code}**",
                  f"  - preview: `{_preview(r.text)}`"]
    except Exception as e:
        lines += [f"- /competitions error: {e}"]
    # /competitions/PL/matches
    try:
        r = requests.get(f"{FD_BASE}/competitions/PL/matches", headers=hdr, timeout=40)
        lines += [f"- /competitions/PL/matches status: **{r.status_code}**",
                  f"  - preview: `{_preview(r.text)}`"]
    except Exception as e:
        lines += [f"- /competitions/PL/matches error: {e}"]

    _md_write(lines)
    return 0

if __name__ == "__main__":
    sys.exit(main())