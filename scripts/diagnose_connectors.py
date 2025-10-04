#!/usr/bin/env python3
"""
diagnose_connectors.py — forensic probe to explain why smoke returned zeros.

It prints:
- Key presence (masked SET/NOT SET)
- League discovery outcome (rows in discovered_leagues.csv)
- Direct HTTP checks:
    API-Football: GET /fixtures?league=39&season=<current>
    FD.org:       GET /competitions (and PL matches)

Writes:
  reports/CONNECTOR_DIAG.md
"""

import os, sys, json, csv
from datetime import datetime, timezone, date
import requests

REP = "reports"; os.makedirs(REP, exist_ok=True)
OUT = os.path.join(REP, "CONNECTOR_DIAG.md")

AF_BASE = "https://v3.football.api-sports.io"
FD_BASE = "https://api.football-data.org/v4"

def today(): return date.today()
def iso(d): return d.strftime("%Y-%m-%d")

def md_print(lines):
    with open(OUT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"diagnose_connectors: wrote {OUT}")

def main():
    lines = ["# CONNECTOR DIAG", f"_Generated: {datetime.utcnow().isoformat()}Z_", ""]

    # --- presence checks (masked) ---
    api_football_key = os.environ.get("API_FOOTBALL_KEY","").strip()
    fdorg_token      = os.environ.get("FDORG_TOKEN","").strip()
    lines += ["## Secret presence", ""]
    lines += [f"- API_FOOTBALL_KEY: {'SET' if api_football_key else 'NOT SET'}"]
    lines += [f"- FDORG_TOKEN: {'SET' if fdorg_token else 'NOT SET'}", ""]

    # --- discovery outcome (if exists) ---
    disco_csv = "data/discovered_leagues.csv"
    rows = []
    if os.path.exists(disco_csv):
        try:
            with open(disco_csv, newline="", encoding="utf-8") as fh:
                for i, r in enumerate(csv.DictReader(fh)):
                    rows.append(r)
                    if i >= 9: break
            lines += ["## Discovery sample (first 10)", ""]
            if rows:
                lines += ["| league_id | league_name | country | season | type |", "|---:|---|---|---:|---|"]
                for r in rows:
                    lines.append(f"| {r.get('league_id','')} | {r.get('league_name','')} | {r.get('country','')} | {r.get('season','')} | {r.get('type','')} |")
            else:
                lines += ["- (discovered_leagues.csv exists but no rows)"]
        except Exception as e:
            lines += [f"- Failed reading discovered_leagues.csv: {e}"]
    else:
        lines += ["## Discovery sample", "", "- (no discovered_leagues.csv found)", ""]
    lines.append("")

    # --- Direct HTTP checks ---
    lines += ["## Direct HTTP checks", ""]

    # API-Football: /fixtures for EPL league=39 (as a sanity sample)
    if api_football_key:
        try:
            h = {"x-apisports-key": api_football_key}
            params = {"league": 39, "season": today().year}
            r = requests.get(f"{AF_BASE}/fixtures", headers=h, params=params, timeout=40)
            body_preview = r.text[:200].replace("\n"," ")
            lines += [f"- API-Football /fixtures status: **{r.status_code}**",
                      f"  - preview: `{body_preview}`"]
        except Exception as e:
            lines += [f"- API-Football request error: {e}"]
    else:
        lines += ["- API-Football: (skipped — key not set)"]

    # FD.org: /competitions and PL matches
    try:
        hdr = {"X-Auth-Token": fdorg_token} if fdorg_token else {}
        r = requests.get(f"{FD_BASE}/competitions", headers=hdr, timeout=40)
        preview = r.text[:200].replace("\n"," ")
        lines += [f"- FD.org /competitions status: **{r.status_code}**",
                  f"  - preview: `{preview}`"]
    except Exception as e:
        lines += [f"- FD.org /competitions error: {e}"]

    try:
        hdr = {"X-Auth-Token": fdorg_token} if fdorg_token else {}
        r = requests.get(f"{FD_BASE}/competitions/PL/matches", headers=hdr, timeout=40)
        preview = r.text[:200].replace("\n"," ")
        lines += [f"- FD.org /competitions/PL/matches status: **{r.status_code}**",
                  f"  - preview: `{preview}`"]
    except Exception as e:
        lines += [f"- FD.org /competitions/PL/matches error: {e}"]

    md_print(lines)
    return 0

if __name__ == "__main__":
    sys.exit(main())