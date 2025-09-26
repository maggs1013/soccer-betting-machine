#!/usr/bin/env python3
"""
multi_odds_fetch.py — The Odds API with two-key fallback; exports H2H + OU + BTTS + Spreads
Writes: data/UPCOMING_fixtures.csv (schema includes H2H & extra markets)
Alerts: reports/ODDS_ALERTS.md
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
    try:
        return datetime.fromisoformat(s.replace("Z","+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def fetch_with_key(key: str) -> List[Dict[str,Any]]:
    rows = []
    if not key:
        return rows
    idx = requests.get(f"{BASE}/sports", params={"apiKey": key}, timeout=20)
    if idx.status_code != 200:
        return rows

    for lg in TARGET_LEAGUES:
        ev = requests.get(f"{BASE}/sports/{lg}/odds", params={
            "apiKey": key,
            "regions": "eu,uk,us",
            "markets": "h2h,totals,btts,spreads",
            "oddsFormat": "decimal",
            "dateFormat": "iso"
        }, timeout=40)
        if ev.status_code != 200:
            continue
        for e in ev.json() or []:
            ko = iso_to_dt(e.get("commence_time") or "")
            if not ko:
                continue
            home = (e.get("home_team") or "").strip()
            away = (e.get("away_team") or "").strip()
            books = e.get("bookmakers") or []
            disp = len(books)

            # Initialize fields
            home_odds = draw_odds = away_odds = ""
            ou_total = ou_over = ou_under = ""
            btts_yes = btts_no = ""
            spread_home_line = spread_home_price = ""
            spread_away_line = spread_away_price = ""
            has_open = has_close = 0

            for b in books:
                for mk in (b.get("markets") or []):
                    keym = mk.get("key")
                    lu = iso_to_dt(mk.get("last_update") or "")
                    if lu and ko:
                        h2k = (ko - lu).total_seconds()/3600.0
                        if h2k >= 48: has_open = 1
                        if h2k <= 3:  has_close = 1

                    outs = mk.get("outcomes") or []
                    if keym == "h2h":
                        for outc in outs:
                            nm, price = outc.get("name"), outc.get("price")
                            if nm == home: home_odds = price
                            elif nm == away: away_odds = price
                            elif nm in ("Draw","Tie"): draw_odds = price

                    elif keym == "totals":
                        # Choose the "main" total: pick the first entry with both over/under
                        main_total = None
                        over = under = ""
                        for outc in outs:
                            if outc.get("name","").lower() == "over":
                                main_total = outc.get("point", main_total)
                                over = outc.get("price")
                        for outc in outs:
                            if outc.get("name","").lower() == "under" and (main_total is None or main_total == outc.get("point")):
                                main_total = outc.get("point", main_total)
                                under = outc.get("price")
                        if main_total is not None and (ou_total == "" or ou_over == "" or ou_under == ""):
                            ou_total = main_total; ou_over = over; ou_under = under

                    elif keym == "btts":
                        # Expect values 'Yes'/'No'
                        for outc in outs:
                            nm, price = (outc.get("name") or "").lower(), outc.get("price")
                            if nm == "yes": btts_yes = price
                            elif nm == "no": btts_no = price

                    elif keym == "spreads":
                        # Expect outcomes with names = home/away; pick the first (main) line
                        h_line = a_line = h_price = a_price = ""
                        for outc in outs:
                            if outc.get("name") == home:
                                h_line = outc.get("point"); h_price = outc.get("price")
                            elif outc.get("name") == away:
                                a_line = outc.get("point"); a_price = outc.get("price")
                        if h_line != "" or a_line != "":
                            spread_home_line = h_line; spread_home_price = h_price
                            spread_away_line = a_line; spread_away_price = a_price

            rows.append({
                "date": ko.isoformat(),
                "home_team": home, "away_team": away,
                "home_odds_dec": home_odds, "draw_odds_dec": draw_odds, "away_odds_dec": away_odds,
                "league": lg, "bookmaker_count": disp,
                "has_opening_odds": has_open, "has_closing_odds": has_close,
                # Extras:
                "ou_main_total": ou_total, "ou_over_price": ou_over, "ou_under_price": ou_under,
                "btts_yes_price": btts_yes, "btts_no_price": btts_no,
                "spread_home_line": spread_home_line, "spread_home_price": spread_home_price,
                "spread_away_line": spread_away_line, "spread_away_price": spread_away_price,
            })
    return rows

def write_alerts(lines: List[str], total: int=0):
    os.makedirs(REPORTS_DIR, exist_ok=True)
    with open(os.path.join(REPORTS_DIR,"ODDS_ALERTS.md"), "w", encoding="utf-8") as f:
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
    alerts, rows = [], []
    # Try primary
    rows = fetch_with_key(os.environ.get("THE_ODDS_API_KEY","").strip())
    if rows:
        alerts.append("✅ Used THE_ODDS_API_KEY")
    else:
        # Fallback to backup
        rows = fetch_with_key(os.environ.get("THE_ODDS_API_KEY_BACKUP","").strip())
        if rows:
            alerts.append("✅ Used THE_ODDS_API_KEY_BACKUP (fallback)")
        else:
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