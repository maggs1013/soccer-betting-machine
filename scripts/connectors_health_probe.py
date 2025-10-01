#!/usr/bin/env python3
"""
connectors_health_probe.py — run smoke tests and write CONNECTOR_HEALTH.md
"""
import os, json
from datetime import datetime

# Import the smoke modules
import connectors.api_football_connect_smoke as api_football
import connectors.football_data_org_connect_smoke as fdorg

REP = "reports"
os.makedirs(REP, exist_ok=True)
OUT = os.path.join(REP, "CONNECTOR_HEALTH.md")

def main():
    lines = ["# CONNECTOR HEALTH", f"_Generated: {datetime.utcnow().isoformat()}Z_", ""]

    # API-Football
    try:
        a = api_football.smoke()
    except Exception as e:
        a = {"source":"api_football","ok":False,"errors":[f"smoke exc={e}"]}

    # Football-Data.org
    try:
        f = fdorg.smoke()
    except Exception as e:
        f = {"source":"football_data_org","ok":False,"errors":[f"smoke exc={e}"]}

    # Render
    lines += ["## API-Football", ""]
    lines += [f"- Leagues: {', '.join(a.get('leagues', [])) or '(none)'}"]
    lines += [f"- Fixtures next 7d: **{a.get('fixtures_7d_total',0)}**"]
    lines += [f"- Injuries last 14d: **{a.get('injuries_14d_total',0)}**"]
    if a.get("errors"):
        lines += ["- Errors:"] + [f"  - {e}" for e in a["errors"]]
    lines.append("")

    lines += ["## Football-Data.org", ""]
    lines += [f"- Competitions: **{f.get('competitions',0)}**"]
    lines += [f"- Matches next 7d: **{f.get('matches_next7d',0)}**"]
    lines += [f"- Matches past 30d: **{f.get('matches_past30d',0)}**"]
    lines += [f"- Standings endpoints OK: **{f.get('standings',0)}** (PL/CL trials)"]
    lines += [f"- Scorers (PL) count: **{f.get('scorers',0)}**"]
    if f.get("errors"):
        lines += ["- Errors:"] + [f"  - {e}" for e in f["errors"]]
    lines.append("")

    with open(OUT, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"connectors_health_probe: wrote {OUT}")

if __name__ == "__main__":
    main()