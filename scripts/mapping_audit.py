#!/usr/bin/env python3
"""
mapping_audit.py — sanity check for alias coverage
- Ensures teams/leagues appearing in fixtures have mappings
- Writes reports/MAPPING_AUDIT.md with missing items
"""

import os
import pandas as pd

DATA = "data"
REPORTS = "reports"
FIX = os.path.join(DATA, "UPCOMING_fixtures.csv")
TEAM_MAP = os.path.join(DATA, "team_name_map.csv")
LEAGUE_MAP = os.path.join(DATA, "league_name_map.csv")
ALLOWLIST = os.path.join(DATA, "leagues_allowlist.csv")
OUT = os.path.join(REPORTS, "MAPPING_AUDIT.md")

def safe_read_csv(path, **kw):
    if not os.path.exists(path): return pd.DataFrame()
    try: return pd.read_csv(path, **kw)
    except Exception: return pd.DataFrame()

def main():
    os.makedirs(REPORTS, exist_ok=True)
    fx = safe_read_csv(FIX)
    tm = safe_read_csv(TEAM_MAP)
    lm = safe_read_csv(LEAGUE_MAP)
    allow = safe_read_csv(ALLOWLIST)

    lines = [f"# MAPPING AUDIT\n"]
    if fx.empty:
        lines.append("- No fixtures found; nothing to audit.\n")
        with open(OUT, "w", encoding="utf-8") as f: f.write("\n".join(lines))
        print("mapping_audit: no fixtures")
        return

    if "home_team" not in fx.columns or "away_team" not in fx.columns:
        lines.append("- Fixtures missing team columns; cannot audit.")
    else:
        teams = pd.unique(pd.concat([fx["home_team"], fx["away_team"]], ignore_index=True).dropna().astype(str).str.strip())
        mapped = set(tm["source_name"].astype(str).str.strip()) if not tm.empty and "source_name" in tm.columns else set()
        missing_teams = sorted([t for t in teams if t not in mapped])
        if missing_teams:
            lines.append("## Missing team mappings")
            for t in missing_teams: lines.append(f"- {t}")
            lines.append("")
        else:
            lines.append("- All fixture teams are present in team_name_map.csv ✅")

    if "league" in fx.columns:
        leagues = pd.unique(fx["league"].dropna().astype(str).str.strip())
        mapped_lg = set(lm["source_league"].astype(str).str.strip()) if not lm.empty and "source_league" in lm.columns else set()
        missing_leagues = sorted([l for l in leagues if l not in mapped_lg])
        if missing_leagues:
            lines.append("## Missing league mappings")
            for l in missing_leagues: lines.append(f"- {l}")
            lines.append("")
        else:
            lines.append("- All fixture leagues are present in league_name_map.csv ✅")
    else:
        lines.append("- Fixtures missing 'league' column; cannot audit leagues.")

    with open(OUT, "w", encoding="utf-8") as f: f.write("\n".join(lines))
    print(f"mapping_audit: wrote {OUT}")

if __name__ == "__main__":
    main()