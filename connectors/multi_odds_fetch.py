#!/usr/bin/env python3
"""
Multi-key Odds/Fixtures fetcher for SBM (The Odds API)
- Tries THE_ODDS_API_KEY first, falls back to THE_ODDS_API_KEY_BACKUP
- Writes:
    data/UPCOMING_fixtures.csv             (compat: one row per game with H2H snapshot)
    data/odds_markets_upcoming.csv         (normalized markets: h2h/ou/btts/spreads)
- Alerts:
    reports/ODDS_ALERTS.md  (includes which key was used)
"""

import os, csv, requests
from datetime import datetime, timezone
from typing import List, Dict, Any

DATA_DIR = "data"
REPORTS_DIR = "reports"
OUT_FIXTURES = os.path.join(DATA_DIR, "UPCOMING_fixtures.csv")
OUT_ODDS     = os.path.join(DATA_DIR, "odds_markets_upcoming.csv")
CACHE_FIXTURES = os.path.join(DATA_DIR, "UPCOMING_fixtures.cache.csv")
ALERTS = os.path.join(REPORTS_DIR, "ODDS_ALERTS.md")

TARGET_LEAGUES = [
    "soccer_epl","soccer_spain_la_liga","soccer_italy_serie_a",
    "soccer_germany_bundesliga","soccer_france_ligue_one",
    "soccer_uefa_champs_league","soccer_uefa_europa_league",
    "soccer_uefa_europa_conference_league","soccer_usa_mls",
    "soccer_netherlands_eredivisie","soccer_portugal_primeira_liga",
    "soccer_belgium_first_div"
]
BASE = "https://api.the-odds-api.com/v4"

def iso_to_dt(s: str):
    try:
        return datetime.fromisoformat(s.replace("Z","+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def fetch_with_key(key: str) -> tuple[list[dict], list[dict], str]:
    fixtures: List[Dict[str,Any]] = []
    odds_rows: List[Dict[str,Any]] = []
    if not key:
        return fixtures, odds_rows, "MISSING_KEY"

    idx = requests.get(f"{BASE}/sports", params={"apiKey": key}, timeout=20)
    if idx.status_code != 200:
        return fixtures, odds_rows, f"IDX_HTTP_{idx.status_code}"

    for lg in TARGET_LEAGUES:
        ev = requests.get(f"{BASE}/sports/{lg}/odds", params={
            "apiKey": key, "regions": "eu,uk,us",
            "markets": "h2h,totals,btts,spreads",
            "oddsFormat": "decimal", "dateFormat": "iso"
        }, timeout=30)
        if ev.status_code != 200:
            continue
        for e in ev.json() or []:
            ko = iso_to_dt(e.get("commence_time") or "")
            if not ko: continue
            home, away = e.get("home_team","").strip(), e.get("away_team","").strip()
            books = e.get("bookmakers") or []

            # H2H snapshot row (compat)
            home_odds = draw_odds = away_odds = ""
            for b in books:
                for mk in b.get("markets") or []:
                    if mk.get("key") == "h2h":
                        for outc in mk.get("outcomes") or []:
                            nm, price = outc.get("name"), outc.get("price")
                            if nm == home: home_odds = price
                            elif nm == away: away_odds = price
                            elif nm in ("Draw","Tie"): draw_odds = price
            fixtures.append({
                "date": ko.isoformat(),
                "home_team": home, "away_team": away,
                "home_odds_dec": home_odds, "draw_odds_dec": draw_odds, "away_odds_dec": away_odds,
                "league": lg, "bookmaker_count": len(books),
                "has_opening_odds": 0, "has_closing_odds": 0
            })

            # Normalized market rows
            for b in books:
                bname = b.get("title") or b.get("key") or "unknown"
                for mk in (b.get("markets") or []):
                    mkey = mk.get("key")          # h2h | totals | btts | spreads
                    for outc in (mk.get("outcomes") or []):
                        rec = {
                            "date": ko.isoformat(),
                            "league": lg,
                            "home_team": home,
                            "away_team": away,
                            "bookmaker": bname,
                            "market": mkey,
                            "label": outc.get("name"),   # e.g., Home / Away / Draw / Over / Under / Yes / No / +0.5 / -0.5
                            "price_dec": outc.get("price"),
                            "point": outc.get("point")   # OU line or spread if present
                        }
                        odds_rows.append(rec)

    return fixtures, odds_rows, "OK"

def write_csv_rows(rows: List[Dict[str,Any]], path: str, header: list[str] | None = None):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not rows:
        if header:
            with open(path, "w", encoding="utf-8") as f:
                f.write(",".join(header) + "\n")
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader(); w.writerows(rows)

def append_alerts(lines: List[str], total: int = 0):
    os.makedirs(REPORTS_DIR, exist_ok=True)
    with open(ALERTS, "w", encoding="utf-8") as f:
        ts = datetime.utcnow().isoformat()
        f.write(f"# ODDS ALERTS — {ts} UTC\n\n")
        for a in lines: f.write(f"- {a}\n")
        f.write(f"\n**Total fixtures (H2H snapshot rows):** {total}\n")

def main():
    alerts: List[str] = []
    # Try primary
    f1, o1, st1 = fetch_with_key(os.environ.get("THE_ODDS_API_KEY","").strip())
    if f1:
        alerts.append("✅ Used THE_ODDS_API_KEY")
        write_csv_rows(f1, OUT_FIXTURES, header=[
            "date","home_team","away_team","home_odds_dec","draw_odds_dec","away_odds_dec","league","bookmaker_count","has_opening_odds","has_closing_odds"
        ])
        write_csv_rows(o1, OUT_ODDS, header=[
            "date","league","home_team","away_team","bookmaker","market","label","price_dec","point"
        ])
        append_alerts(alerts, total=len(f1))
        print(f"✅ Odds (primary) wrote {len(f1)} fixtures → {OUT_FIXTURES} and {len(o1)} market rows → {OUT_ODDS}")
        return

    # Fallback
    f2, o2, st2 = fetch_with_key(os.environ.get("THE_ODDS_API_KEY_BACKUP","").strip())
    if f2:
        alerts.append("✅ Used THE_ODDS_API_KEY_BACKUP (fallback)")
        write_csv_rows(f2, OUT_FIXTURES, header=[
            "date","home_team","away_team","home_odds_dec","draw_odds_dec","away_odds_dec","league","bookmaker_count","has_opening_odds","has_closing_odds"
        ])
        write_csv_rows(o2, OUT_ODDS, header=[
            "date","league","home_team","away_team","bookmaker","market","label","price_dec","point"
        ])
        append_alerts(alerts, total=len(f2))
        print(f"✅ Odds (backup) wrote {len(f2)} fixtures → {OUT_FIXTURES} and {len(o2)} market rows → {OUT_ODDS}")
        return

    alerts.append(f"❌ Both odds keys failed (primary:{st1}, backup:{st2})")
    # Fallback to cache for fixtures only
    if os.path.exists(CACHE_FIXTURES):
        with open(CACHE_FIXTURES, "r", encoding="utf-8") as src, open(OUT_FIXTURES, "w", encoding="utf-8") as dst:
            dst.write(src.read())
        alerts.append("ℹ️ Using cached fixtures for compatibility")
        append_alerts(alerts, total=0)
        print("Wrote cached fixtures")
        return

    # empty files to avoid downstream crashes
    write_csv_rows([], OUT_FIXTURES, header=[
        "date","home_team","away_team","home_odds_dec","draw_odds_dec","away_odds_dec","league","bookmaker_count","has_opening_odds","has_closing_odds"
    ])
    write_csv_rows([], OUT_ODDS, header=[
        "date","league","home_team","away_team","bookmaker","market","label","price_dec","point"
    ])
    append_alerts(alerts, total=0)
    print("No fixtures available")

if __name__ == "__main__":
    main()