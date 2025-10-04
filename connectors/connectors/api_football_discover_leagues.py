#!/usr/bin/env python3
"""
api_football_discover_leagues.py — discover active API-Football leagues and export IDs for this run.

What it does
------------
- Calls API-Football /leagues?current=true to list active competitions.
- Filters to Football (soccer), type=League (skips cups unless requested).
- Optional filters by countries or seasons via envs.
- Writes:
    data/discovered_leagues.csv   (id, name, country, season, type)
    reports/LEAGUE_DISCOVERY.md   (counts, top few)
- Exports API_FOOTBALL_LEAGUE_IDS=comma,separated,ids to GITHUB_ENV for this job.

Env required
------------
API_FOOTBALL_KEY   (GitHub Secret)

Optional envs
-------------
API_FOOTBALL_DISCOVER_COUNTRIES   CSV of country names (e.g. "England,Spain,Italy") to restrict; empty=all
API_FOOTBALL_DISCOVER_TYPE        "league" (default) or "all" (include cups)
API_FOOTBALL_DISCOVER_SEASONS     CSV of years; default = current year only
MAX_DISCOVERED_LEAGUES            default "60" (cap to be respectful of rate limits / your quotas)

Notes
-----
- Safe to run every job. If discovery fails, it exits 0 and leaves env unchanged;
  downstream steps can still use a static override in repo variables if present.
"""

import os, sys, json, csv, requests
from datetime import datetime, timezone

DATA = "data"
REP  = "reports"
os.makedirs(DATA, exist_ok=True)
os.makedirs(REP, exist_ok=True)

OUT_CSV = os.path.join(DATA, "discovered_leagues.csv")
OUT_MD  = os.path.join(REP,  "LEAGUE_DISCOVERY.md")

BASE = "https://v3.football.api-sports.io"

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def write_env(name, value):
    ghe = os.environ.get("GITHUB_ENV")
    if ghe:
        with open(ghe, "a", encoding="utf-8") as fh:
            fh.write(f"{name}={value}\n")

def main():
    key = os.environ.get("API_FOOTBALL_KEY", "").strip()
    if not key:
        print("discover_leagues: API_FOOTBALL_KEY missing; skipping discovery.")
        # don't fail the job — just skip discovery
        return 0

    countries_csv = os.environ.get("API_FOOTBALL_DISCOVER_COUNTRIES", "").strip()
    countries = [c.strip().lower() for c in countries_csv.split(",") if c.strip()] if countries_csv else []
    type_filter = os.environ.get("API_FOOTBALL_DISCOVER_TYPE", "league").strip().lower()  # "league" or "all"
    seasons_csv = os.environ.get("API_FOOTBALL_DISCOVER_SEASONS", "").strip()
    seasons = [s.strip() for s in seasons_csv.split(",") if s.strip()] if seasons_csv else []
    max_n = int(os.environ.get("MAX_DISCOVERED_LEAGUES", "60"))

    headers = {"x-apisports-key": key}
    params  = {"current": "true"}  # active competitions

    try:
        r = requests.get(f"{BASE}/leagues", headers=headers, params=params, timeout=45)
        if r.status_code != 200:
            print(f"discover_leagues: status={r.status_code} body={r.text[:200]}")
            return 0
        resp = r.json() or {}
        items = resp.get("response", [])
    except Exception as e:
        print(f"discover_leagues: request failed: {e}")
        return 0

    rows = []
    for it in items:
        lg  = it.get("league") or {}
        cn  = it.get("country") or {}
        ssn = it.get("seasons") or []
        sport = (lg.get("sport") or "Soccer").lower()
        if sport not in ("soccer","football"):
            continue
        # type filter
        typ = (lg.get("type") or "").lower()
        if type_filter == "league" and typ != "league":
            continue
        # season list
        for s in ssn:
            year = s.get("year")
            current = s.get("current")
            if seasons and str(year) not in seasons:
                continue
            if not seasons and not current:
                # default to current year only
                continue
            country = (cn.get("name") or "").strip()
            if countries and country.lower() not in countries:
                continue
            rows.append({
                "league_id": lg.get("id"),
                "league_name": lg.get("name"),
                "country": country,
                "season": year,
                "type": lg.get("type"),
            })

    # de-dup by league_id (prefer current season)
    seen = set()
    dedup = []
    for r in rows:
        lid = r.get("league_id")
        if lid in seen: 
            continue
        seen.add(lid)
        dedup.append(r)

    # cap to max_n to avoid huge pulls on free/medium plans
    dedup = dedup[:max_n]

    # write CSV
    try:
        with open(OUT_CSV, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=["league_id","league_name","country","season","type"])
            w.writeheader()
            for r in dedup:
                w.writerow(r)
    except Exception as e:
        print("discover_leagues: failed writing CSV:", e)

    # write MD
    lines = ["# LEAGUE DISCOVERY", f"_Generated: {now_iso()}_", ""]
    lines.append(f"- Selected leagues: **{len(dedup)}** (cap={max_n})")
    if countries:
        lines.append(f"- Country filter: {', '.join(countries)}")
    lines.append("")
    if dedup:
        lines += ["| ID | League | Country | Season | Type |", "|---:|---|---|---:|---|"]
        for r in dedup[:30]:
            lines.append(f"| {r['league_id']} | {r['league_name']} | {r['country']} | {r['season']} | {r['type']} |")
        if len(dedup) > 30:
            lines.append(f"... and {len(dedup)-30} more")
    else:
        lines.append("- (No leagues matched filters or API returned none.)")

    try:
        with open(OUT_MD, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
    except Exception as e:
        print("discover_leagues: failed writing MD:", e)

    # export env for downstream steps in this run
    ids = ",".join(str(r["league_id"]) for r in dedup if r.get("league_id"))
    if ids:
        write_env("API_FOOTBALL_LEAGUE_IDS", ids)
        print("discover_leagues: exported API_FOOTBALL_LEAGUE_IDS to job env")
    else:
        print("discover_leagues: found 0 league ids; env not set")

    return 0

if __name__ == "__main__":
    sys.exit(main())