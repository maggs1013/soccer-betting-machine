#!/usr/bin/env python3
"""
Multi-provider Odds/Fixtures fetcher for SBM
Order:
  1) The Odds API
  2) API-FOOTBALL (fixtures + odds)
Writes a unified CSV: data/UPCOMING_fixtures.csv
Emits alerts to reports/ODDS_ALERTS.md
"""

import os, csv, time, math, requests
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

DATA_DIR = "data"
REPORTS_DIR = "reports"
OUT_FIXTURES = os.path.join(DATA_DIR, "UPCOMING_fixtures.csv")
CACHE_FIXTURES = os.path.join(DATA_DIR, "UPCOMING_fixtures.cache.csv")
ALERTS = os.path.join(REPORTS_DIR, "ODDS_ALERTS.md")

TARGET_LEAGUES_ODDSAPI = [
    "soccer_epl","soccer_spain_la_liga","soccer_italy_serie_a",
    "soccer_germany_bundesliga","soccer_france_ligue_one",
    "soccer_uefa_champs_league","soccer_uefa_europa_league",
    "soccer_uefa_europa_conference_league","soccer_usa_mls",
    "soccer_netherlands_eredivisie","soccer_portugal_primeira_liga",
    "soccer_belgium_first_div"
]

# Default API-FOOTBALL league ids (edit with repo VAR: API_FOOTBALL_LEAGUE_IDS)
API_FOOTBALL_DEFAULTS = [
    39,   # EPL
    140,  # La Liga
    135,  # Serie A
    78,   # Bundesliga
    61,   # Ligue 1
    2,    # UEFA CL
    3,    # UEFA EL
    848,  # UEFA ECL (check id)
    253,  # MLS (check id)
    88,   # Eredivisie
    94,   # Primeira Liga
    144   # Belgium First Division A
]

def write_alerts(lines: List[str], total: int|None=None, by_league: Dict[str,int]|None=None):
    os.makedirs(REPORTS_DIR, exist_ok=True)
    with open(ALERTS, "w", encoding="utf-8") as f:
        ts = datetime.utcnow().isoformat()
        f.write(f"# ODDS ALERTS — {ts} UTC\n\n")
        for a in lines: f.write(f"- {a}\n")
        if total is not None: f.write(f"\n**Total events fetched:** {total}\n")
        if by_league:
            f.write("\n**Events by league:**\n")
            for lg, n in sorted(by_league.items()): f.write(f"- {lg}: {n}\n")

def write_rows(rows: List[Dict[str,Any]], path: str):
    os.makedirs(DATA_DIR, exist_ok=True)
    if not rows:
        # keep header for downstream
        with open(path, "w", encoding="utf-8") as f:
            f.write("date,home_team,away_team,home_odds_dec,draw_odds_dec,away_odds_dec,league,bookmaker_count,has_opening_odds,has_closing_odds\n")
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader(); w.writerows(rows)

# ---------- Provider 1: The Odds API ----------

def fetch_oddsapi(key: str) -> List[Dict[str,Any]]:
    BASE = "https://api.the-odds-api.com/v4"
    rows, alerts = [], []
    # sports index
    r = requests.get(f"{BASE}/sports", params={"apiKey": key}, timeout=30)
    if r.status_code != 200:
        alerts.append(f"❌ The Odds API sports index failed: HTTP {r.status_code}")
        return rows  # empty → caller will try next provider

    def iso_to_dt(s: str):
        try: return datetime.fromisoformat(s.replace("Z","+00:00")).astimezone(timezone.utc)
        except Exception: return None

    for lg in TARGET_LEAGUES_ODDSAPI:
        ev = requests.get(f"{BASE}/sports/{lg}/odds", params={
            "apiKey": key, "regions": "eu,uk,us",
            "markets": "h2h,totals,btts,spreads",
            "oddsFormat": "decimal", "dateFormat": "iso"
        }, timeout=30)
        if ev.status_code != 200:
            alerts.append(f"⚠️ {lg}: HTTP {ev.status_code}")
            continue
        events = ev.json() or []
        for e in events:
            ko = iso_to_dt(e.get("commence_time") or "")
            if not ko: continue
            home = (e.get("home_team") or "").strip()
            away = (e.get("away_team") or "").strip()
            books = e.get("bookmakers") or []
            disp, has_open, has_close = len(books), 0, 0

            for b in books:
                for mk in (b.get("markets") or []):
                    if mk.get("key") not in ("h2h","totals","btts","spreads"): continue
                    lu = iso_to_dt(mk.get("last_update") or "")
                    if lu:
                        h2k = (ko - lu).total_seconds()/3600.0
                        if h2k >= 48: has_open = 1
                        if h2k <= 3:  has_close = 1

            home_odds = draw_odds = away_odds = ""
            for b in books:
                for mk in (b.get("markets") or []):
                    if mk.get("key") != "h2h": continue
                    for outc in (mk.get("outcomes") or []):
                        if outc.get("name") == home: home_odds = outc.get("price")
                        elif outc.get("name") == away: away_odds = outc.get("price")
                        elif outc.get("name") in ("Draw","Tie"): draw_odds = outc.get("price")

            rows.append({
                "date": ko.isoformat(),
                "home_team": home,
                "away_team": away,
                "home_odds_dec": home_odds,
                "draw_odds_dec": draw_odds,
                "away_odds_dec": away_odds,
                "league": lg,
                "bookmaker_count": disp,
                "has_opening_odds": has_open,
                "has_closing_odds": has_close
            })
    return rows

# ---------- Provider 2: API-FOOTBALL (fixtures + odds) ----------

def fetch_api_football(key: str) -> List[Dict[str,Any]]:
    """
    Uses:
      GET https://v3.football.api-sports.io/fixtures?league={id}&season={season}&from={YYYY-MM-DD}&to={YYYY-MM-DD}
      GET https://v3.football.api-sports.io/odds?fixture={fixtureId}
    """
    if not key: return []
    headers = {"x-apisports-key": key}
    today = datetime.utcnow().date()
    date_from = today
    date_to = today + timedelta(days=7)
    season = _infer_season(today)

    # League ID list from env (comma-separated) or defaults
    env_ids = os.environ.get("API_FOOTBALL_LEAGUE_IDS","").strip()
    league_ids = [int(x) for x in env_ids.split(",") if x.strip().isdigit()] or API_FOOTBALL_DEFAULTS

    rows: List[Dict[str,Any]] = []
    for lid in league_ids:
        # fixtures window
        fx = requests.get(
            "https://v3.football.api-sports.io/fixtures",
            headers=headers,
            params={"league": lid, "season": season, "from": str(date_from), "to": str(date_to)},
            timeout=30
        )
        if fx.status_code != 200:
            continue
        data = (fx.json() or {}).get("response") or []
        for m in data:
            fixture = m.get("fixture", {})
            teams = m.get("teams", {})
            home = (teams.get("home", {}).get("name") or "").strip()
            away = (teams.get("away", {}).get("name") or "").strip()
            ts = fixture.get("date")
            try:
                ko = datetime.fromisoformat(ts.replace("Z","+00:00")).astimezone(timezone.utc)
            except Exception:
                continue

            # odds lookup (optional; many books supported)
            fixture_id = fixture.get("id")
            home_odds = draw_odds = away_odds = ""
            disp = has_open = has_close = 0

            if fixture_id:
                od = requests.get(
                    "https://v3.football.api-sports.io/odds",
                    headers=headers,
                    params={"fixture": fixture_id},
                    timeout=30
                )
                if od.status_code == 200:
                    odresp = (od.json() or {}).get("response") or []
                    # count bookmakers; scan main line for H2H if present
                    if odresp:
                        # Each bookmaker has bets; look for "Match Winner" (id varies; fallback scan by name)
                        for bm in odresp:
                            disp += 1
                            for bet in bm.get("bets") or []:
                                name = (bet.get("name") or "").lower()
                                if "match winner" in name or name in ("1x2","h2h"):
                                    for v in bet.get("values") or []:
                                        nm = (v.get("value") or "").lower()
                                        odd = v.get("odd")
                                        if nm in ("home","1"): home_odds = odd
                                        elif nm in ("away","2"): away_odds = odd
                                        elif nm in ("draw","x"): draw_odds = odd
                        # opening/closing approximation not always exposed → leave 0/0

            rows.append({
                "date": ko.isoformat(),
                "home_team": home,
                "away_team": away,
                "home_odds_dec": home_odds,
                "draw_odds_dec": draw_odds,
                "away_odds_dec": away_odds,
                "league": f"api-football:{lid}",
                "bookmaker_count": disp,
                "has_opening_odds": has_open,
                "has_closing_odds": has_close
            })
    return rows

def _infer_season(today):
    # Simple season inference for European leagues: season == current year if Aug–Dec else current-1
    y = today.year
    if today.month >= 8:
        return y
    return y - 1

# ---------- Orchestrator ----------

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(REPORTS_DIR, exist_ok=True)
    alerts: List[str] = []

    # Provider 1: The Odds API
    odds_key = os.environ.get("ODDS_API_KEY","").strip()
    rows = []
    if odds_key:
        try:
            rows = fetch_oddsapi(odds_key)
        except Exception as e:
            alerts.append(f"❌ The Odds API fetch crashed: {e}")
    else:
        alerts.append("ℹ️ ODDS_API_KEY not set; skipping The Odds API.")

    # Provider 2: API-FOOTBALL (if needed)
    if not rows:
        key2 = os.environ.get("API_FOOTBALL_KEY","").strip()
        if key2:
            try:
                rows = fetch_api_football(key2)
                if not rows:
                    alerts.append("❌ API-FOOTBALL returned no fixtures/odds in 7d window.")
            except Exception as e:
                alerts.append(f"❌ API-FOOTBALL fetch crashed: {e}")
        else:
            alerts.append("ℹ️ API_FOOTBALL_KEY not set; skipping API-FOOTBALL.")

    # Write outcomes or cache
    if not rows:
        # fallback to cache if available
        if os.path.exists(CACHE_FIXTURES):
            with open(CACHE_FIXTURES, "r", encoding="utf-8") as src, open(OUT_FIXTURES, "w", encoding="utf-8") as dst:
                dst.write(src.read())
            alerts.append("ℹ️ Using cached fixtures: data/UPCOMING_fixtures.cache.csv")
            write_alerts(alerts, total=0, by_league=None)
            print("Wrote cached fixtures")
            return
        # no cache: write empty with header
        write_rows([], OUT_FIXTURES)
        alerts.append("🚫 No providers succeeded and no cache found; fixtures remain empty.")
        write_alerts(alerts, total=0, by_league=None)
        print("No fixtures available")
        return

    # success → write live and cache
    write_rows(rows, OUT_FIXTURES)
    try: write_rows(rows, CACHE_FIXTURES)
    except Exception: pass
    # simple per-league count
    counts: Dict[str,int] = {}
    for r in rows:
        lg = r.get("league") or "unknown"
        counts[lg] = counts.get(lg,0) + 1
    write_alerts(alerts, total=len(rows), by_league=counts)
    print(f"✅ Wrote {len(rows)} fixtures → {OUT_FIXTURES}")

if __name__ == "__main__":
    main()