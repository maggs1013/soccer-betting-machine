#!/usr/bin/env python3
"""
Audit unmapped team/league names so fixtures don't silently drop.
Reads: data/UPCOMING_fixtures.csv, team_name_map.csv, league_name_map.csv
Writes: reports/ALIAS_AUDIT.csv (rows that could not be mapped)
"""

import os, csv

DATA = "data"
REPORTS = "reports"
FIX = os.path.join(DATA, "UPCOMING_fixtures.csv")
TEAM_MAP = os.path.join(DATA, "team_name_map.csv")
LEAGUE_MAP = os.path.join(DATA, "league_name_map.csv")
OUT = os.path.join(REPORTS, "ALIAS_AUDIT.csv")

def load_map(path):
    m = {}
    if os.path.exists(path):
        with open(path,"r",encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                src = (row.get("source_name") or row.get("alias") or row.get("team") or "").strip().lower()
                std = (row.get("standard_name") or row.get("canonical") or row.get("target") or "").strip()
                if src:
                    m[src] = std
    return m

def main():
    os.makedirs(REPORTS, exist_ok=True)
    team_map = load_map(TEAM_MAP)
    league_map = load_map(LEAGUE_MAP)
    rows_out = []

    if not os.path.exists(FIX):
        with open(OUT,"w",encoding="utf-8") as f: f.write("issue,field,value\n")
        print("No fixtures to audit")
        return

    with open(FIX,"r",encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            lg = (row.get("league") or "").strip()
            ht = (row.get("home_team") or "").strip()
            at = (row.get("away_team") or "").strip()
            if lg.lower() not in league_map:
                rows_out.append({"issue":"unmapped_league","field":"league","value":lg})
            if ht.lower() not in team_map:
                rows_out.append({"issue":"unmapped_team","field":"home_team","value":ht})
            if at.lower() not in team_map:
                rows_out.append({"issue":"unmapped_team","field":"away_team","value":at})

    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["issue","field","value"])
        w.writeheader(); w.writerows(rows_out)

    print(f"Alias audit found {len(rows_out)} issues → {OUT}")

if __name__ == "__main__":
    main()