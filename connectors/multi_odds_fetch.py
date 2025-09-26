#!/usr/bin/env python3
"""
multi_odds_fetch.py — two-key fallback + richer fields
- Tries THE_ODDS_API_KEY then THE_ODDS_API_KEY_BACKUP
- Extracts H2H, + picks a main OU line and BTTS if present, and basic spread
- Writes: data/UPCOMING_fixtures.csv (+ cache), reports/ODDS_ALERTS.md
"""

import os, csv, requests
from datetime import datetime, timezone
from typing import List, Dict, Any

DATA_DIR = "data"
REPORTS_DIR = "reports"
OUT_FIXTURES = os.path.join(DATA_DIR, "UPCOMING_fixtures.csv")
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
    try: return datetime.fromisoformat(s.replace("Z","+00:00")).astimezone(timezone.utc)
    except Exception: return None

def choose_ou_line(markets: list) -> Dict[str,Any]:
    # pick totals line closest to 2.5 if available; else first
    lines = []
    for mk in markets or []:
        if mk.get("key") != "totals": continue
        for outc in mk.get("outcomes") or []:
            # outcomes often two entries ("Over X", "Under X") with "point" field
            point = outc.get("point")
            if point is None: continue
            lines.append(point)
    if not lines: return {}
    # choose nearest to 2.5
    target = min(lines, key=lambda x: abs(float(x)-2.5))
    over_price = under_price = ""
    for mk in markets or []:
        if mk.get("key") != "totals": continue
        for outc in mk.get("outcomes") or []:
            if outc.get("point") != target: continue
            nm = (outc.get("name") or "").lower()
            if "over" in nm: over_price = outc.get("price")
            if "under" in nm: under_price = outc.get("price")
    return {"ou_main_total": float(target), "ou_over_price": over_price, "ou_under_price": under_price}

def choose_btts(markets: list) -> Dict[str,Any]:
    res = {}
    for mk in markets or []:
        if mk.get("key") != "btts": continue
        for outc in mk.get("outcomes") or []:
            nm = (outc.get("name") or "").lower()
            if nm == "yes": res["btts_yes_price"] = outc.get("price")
            if nm == "no":  res["btts_no_price"]  = outc.get("price")
    return res

def choose_spread(markets: list, home: str, away: str) -> Dict[str,Any]:
    res = {}
    for mk in markets or []:
        if mk.get("key") != "spreads": continue
        for outc in mk.get("outcomes") or []:
            nm = outc.get("name")
            if nm == home:
                res["spread_home_line"]  = outc.get("point")
                res["spread_home_price"] = outc.get("price")
            elif nm == away:
                res["spread_away_line"]  = outc.get("point")
                res["spread_away_price"] = outc.get("price")
    return res

def fetch_with_key(key: str) -> (List[Dict[str,Any]], str):
    rows = []
    if not key: return rows, "⚠️ key empty"
    idx = requests.get(f"{BASE}/sports", params={"apiKey": key}, timeout=20)
    if idx.status_code != 200:
        return rows, f"❌ sports index {idx.status_code}"

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
            home_odds = draw_odds = away_odds = ""
            disp = len(books)

            # combine all books into a single snapshot by taking first available
            markets_flat = []
            for b in books:
                for mk in (b.get("markets") or []): markets_flat.append(mk)

            # H2H
            for mk in markets_flat:
                if mk.get("key") != "h2h": continue
                for outc in (mk.get("outcomes") or []):
                    nm, price = outc.get("name"), outc.get("price")
                    if nm == home: home_odds = price
                    elif nm == away: away_odds = price
                    elif nm in ("Draw","Tie"): draw_odds = price
                break

            row = {
                "date": ko.isoformat(),
                "home_team": home, "away_team": away,
                "home_odds_dec": home_odds, "draw_odds_dec": draw_odds, "away_odds_dec": away_odds,
                "league": lg, "bookmaker_count": disp,
                "has_opening_odds": 0, "has_closing_odds": 0
            }
            # OU / BTTS / Spreads
            row.update(choose_ou_line(markets_flat))
            row.update(choose_btts(markets_flat))
            row.update(choose_spread(markets_flat, home, away))
            rows.append(row)
    return rows, "ok"

def write_alerts(lines: List[str], total: int=0):
    os.makedirs(REPORTS_DIR, exist_ok=True)
    with open(os.path.join(REPORTS_DIR, "ODDS_ALERTS.md"), "w", encoding="utf-8") as f:
        ts = datetime.utcnow().isoformat()
        f.write(f"# ODDS ALERTS — {ts} UTC\n\n")
        for a in lines: f.write(f"- {a}\n")
        f.write(f"\n**Total fixtures:** {total}\n")

def write_rows(rows: List[Dict[str,Any]], path: str):
    os.makedirs(DATA_DIR, exist_ok=True)
    if not rows:
        with open(path, "w", encoding="utf-8") as f:
            f.write("date,home_team,away_team,home_odds_dec,draw_odds_dec,away_odds_dec,league,bookmaker_count,has_opening_odds,has_closing_odds,ou_main_total,ou_over_price,ou_under_price,btts_yes_price,btts_no_price,spread_home_line,spread_home_price,spread_away_line,spread_away_price\n")
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader(); w.writerows(rows)

def main():
    alerts = []
    # primary
    rows, note1 = fetch_with_key(os.environ.get("THE_ODDS_API_KEY","").strip())
    if rows:
        alerts.append("✅ Used THE_ODDS_API_KEY")
    else:
        if note1 and "❌" in note1: alerts.append(f"{note1}")
        # backup
        rows, note2 = fetch_with_key(os.environ.get("THE_ODDS_API_KEY_BACKUP","").strip())
        if rows:
            alerts.append("✅ Used THE_ODDS_API_KEY_BACKUP (fallback)")
        else:
            if note2 and "❌" in note2: alerts.append(f"{note2}")
            alerts.append("❌ Both odds keys failed")
            if os.path.exists(CACHE_FIXTURES):
                with open(CACHE_FIXTURES,"r",encoding="utf-8") as src, open(OUT_FIXTURES,"w",encoding="utf-8") as dst:
                    dst.write(src.read())
                alerts.append("ℹ️ Using cached fixtures")
                write_alerts(alerts, total=0); return
            write_rows([], OUT_FIXTURES); write_alerts(alerts, total=0); return

    write_rows(rows, OUT_FIXTURES)
    try: write_rows(rows, CACHE_FIXTURES)
    except Exception: pass
    write_alerts(alerts, total=len(rows))
    print(f"✅ Odds wrote {len(rows)} fixtures → {OUT_FIXTURES}")

if __name__ == "__main__":
    main()