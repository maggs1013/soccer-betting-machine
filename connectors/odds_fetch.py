#!/usr/bin/env python3
"""
Resilient Odds/Fixtures fetcher for SBM
- Pulls fixtures + H2H/OU/BTTS/Spreads for target leagues
- Computes bookmaker dispersion and opening/closing reach
- Falls back to cache when API fails or returns too few events
- Writes: data/UPCOMING_fixtures.csv and reports/ODDS_ALERTS.md
"""

import os, sys, math, time, json, csv
from datetime import datetime, timezone
from typing import Dict, Any, List
import requests

TARGET_LEAGUES = [
    "soccer_epl","soccer_spain_la_liga","soccer_italy_serie_a",
    "soccer_germany_bundesliga","soccer_france_ligue_one",
    "soccer_uefa_champs_league","soccer_uefa_europa_league",
    "soccer_uefa_europa_conference_league","soccer_usa_mls",
    "soccer_netherlands_eredivisie","soccer_portugal_primeira_liga",
    "soccer_belgium_first_div"
]
HORIZON_DAYS = 7
MIN_EVENTS_WEEKEND = 20
BASE = "https://api.the-odds-api.com/v4"
REGIONS = "eu,uk,us"
MARKETS = "h2h,totals,btts,spreads"

DATA_DIR = "data"
REPORTS_DIR = "reports"
OUT_FIXTURES = os.path.join(DATA_DIR, "UPCOMING_fixtures.csv")
CACHE_FIXTURES = os.path.join(DATA_DIR, "UPCOMING_fixtures.cache.csv")
ALERTS = os.path.join(REPORTS_DIR, "ODDS_ALERTS.md")

def http_get(url, params, tries=3, sleep=2):
    last = None
    for _ in range(tries):
        try:
            r = requests.get(url, params=params, timeout=30)
            last = r
            if r.status_code < 500:
                return r
        except requests.RequestException as e:
            last = e
        time.sleep(sleep)
    return last

def iso_to_dt(iso: str):
    try:
        return datetime.fromisoformat(iso.replace("Z","+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(REPORTS_DIR, exist_ok=True)

    api_key = os.environ.get("ODDS_API_KEY","").strip()
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    alerts: List[str] = []

    if not api_key:
        alerts.append("❌ ODDS_API_KEY missing; using cache if present.")
        return use_cache_with_alerts(alerts)

    idx = http_get(f"{BASE}/sports", params={"apiKey": api_key})
    if not hasattr(idx, "status_code") or idx.status_code != 200:
        alerts.append(f"❌ Odds sports index failed: {getattr(idx,'status_code',None)} — using cache if present.")
        return use_cache_with_alerts(alerts)

    rows = []
    total_events = 0
    leagues_counts = {}

    for lg in TARGET_LEAGUES:
        ev = http_get(f"{BASE}/sports/{lg}/odds", params={
            "apiKey": api_key, "regions": REGIONS, "markets": MARKETS,
            "oddsFormat": "decimal", "dateFormat": "iso"
        }, tries=2)
        if not hasattr(ev, "status_code") or ev.status_code != 200:
            alerts.append(f"⚠️ {lg}: HTTP {getattr(ev,'status_code',None)}; skipping.")
            continue

        events = ev.json() or []
        leagues_counts[lg] = len(events)
        total_events += len(events)

        for e in events:
            ko = iso_to_dt(e.get("commence_time") or "")
            if not ko:
                continue
            home = (e.get("home_team") or "").strip()
            away = (e.get("away_team") or "").strip()
            books = e.get("bookmakers", []) or []
            dispersion = len(books)
            has_open = has_close = 0
            for b in books:
                for mk in (b.get("markets") or []):
                    if mk.get("key") not in ("h2h","totals","btts","spreads"):
                        continue
                    lu = iso_to_dt(mk.get("last_update") or "")
                    if lu:
                        h2k = (ko - lu).total_seconds()/3600.0
                        if h2k >= 48: has_open = 1
                        if h2k <= 3:  has_close = 1

            home_odds = draw_odds = away_odds = ""
            for b in books:
                for mk in (b.get("markets") or []):
                    if mk.get("key") == "h2h":
                        for outc in (mk.get("outcomes") or []):
                            if outc.get("name") == home: home_odds = outc.get("price")
                            elif outc.get("name") == away: away_odds = outc.get("price")
                            elif outc.get("name") in ("Draw","Tie"): draw_odds = outc.get("price")

            rows.append({
                "date": ko.isoformat(),
                "home_team": home, "away_team": away,
                "home_odds_dec": home_odds, "draw_odds_dec": draw_odds, "away_odds_dec": away_odds,
                "league": lg, "bookmaker_count": dispersion,
                "has_opening_odds": has_open, "has_closing_odds": has_close
            })

    if total_events == 0:
        alerts.append("❌ No odds events found across target leagues; using cache if present.")
        return use_cache_with_alerts(alerts)

    dow = now.weekday()
    if dow in (4,5,6,0) and total_events < MIN_EVENTS_WEEKEND:
        alerts.append(f"⚠️ Low odds coverage on weekend window: {total_events} events.")

    write_csv(rows, OUT_FIXTURES)
    try: write_csv(rows, CACHE_FIXTURES)
    except Exception: pass

    save_alerts(alerts, total_events, leagues_counts)
    print(f"✅ Odds wrote {len(rows)} fixture rows → {OUT_FIXTURES}")

def write_csv(rows, path):
    import csv
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader(); w.writerows(rows)

def use_cache_with_alerts(alerts: List[str]):
    if os.path.exists(CACHE_FIXTURES):
        alerts.append("ℹ️ Using cached fixtures: data/UPCOMING_fixtures.cache.csv")
        with open(CACHE_FIXTURES, "r", encoding="utf-8") as src, open(OUT_FIXTURES, "w", encoding="utf-8") as dst:
            dst.write(src.read())
        save_alerts(alerts, -1, {}); print("Wrote cached fixtures"); return
    alerts.append("🚫 No cache available; fixtures remain empty.")
    save_alerts(alerts, 0, {})
    with open(OUT_FIXTURES, "w", encoding="utf-8") as f:
        f.write("date,home_team,away_team,home_odds_dec,draw_odds_dec,away_odds_dec,league,bookmaker_count,has_opening_odds,has_closing_odds\n")

def save_alerts(alerts: List[str], total: int, leagues_counts: Dict[str,int]):
    os.makedirs(REPORTS_DIR, exist_ok=True)
    with open(ALERTS, "w", encoding="utf-8") as f:
        ts = datetime.utcnow().isoformat()
        f.write(f"# ODDS ALERTS — {ts} UTC\n\n")
        for a in alerts: f.write(f"- {a}\n")
        if total is not None: f.write(f"\n**Total events fetched:** {total}\n")
        if leagues_counts:
            f.write("\n**Events by league:**\n")
            for lg, n in sorted(leagues_counts.items()): f.write(f"- {lg}: {n}\n")

if __name__ == "__main__":
    main()