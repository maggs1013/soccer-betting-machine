# scripts/fetch_the_odds_api.py
# Odds API only. Sweeps multiple soccer sport keys (UCL, UEL, EPL, LaLiga, Serie A, Bundesliga, Ligue 1).
# Writes data/raw_theodds_fixtures.csv with deduped rows (latest wins). Safe if API down.

import os, sys, time, requests, pandas as pd

DATA = "data"
OUT  = os.path.join(DATA, "raw_theodds_fixtures.csv")
BASE = "https://api.the-odds-api.com/v4"

API_KEY = os.environ.get("THE_ODDS_API_KEY","").strip()
REGIONS = os.environ.get("ODDS_REGIONS","eu,uk,us,au").strip()
MARKETS = os.environ.get("ODDS_MARKETS","h2h").strip()

# Common soccer sport keys (expand as needed)
SPORT_KEYS = [
    "soccer_uefa_champs_league",
    "soccer_uefa_europa_league",
    "soccer_uefa_europa_conference_league",
    "soccer_epl", "soccer_spain_la_liga", "soccer_italy_serie_a",
    "soccer_germany_bundesliga", "soccer_france_ligue_one"
]

def write_empty(msg):
    pd.DataFrame(columns=["date","home_team","away_team","home_odds_dec","draw_odds_dec","away_odds_dec"]).to_csv(OUT,index=False)
    print("INFO:", msg, "| wrote empty", OUT)
    sys.exit(0)

def fetch_key(sk):
    url = f"{BASE}/sports/{sk}/odds"
    r = requests.get(url, params={"apiKey":API_KEY,"regions":REGIONS,"markets":MARKETS,"oddsFormat":"decimal"}, timeout=60)
    if r.status_code!=200:
        print(f"[WARN] odds fetch {sk} → {r.status_code} {r.text[:90]}")
        return []
    rows=[]
    for g in r.json():
        home, away, dt = g.get("home_team"), g.get("away_team"), g.get("commence_time")
        H=D=A=None
        for bm in g.get("bookmakers", []):
            for mk in bm.get("markets", []):
                if mk.get("key")=="h2h":
                    m = {o.get("name"): float(o.get("price")) for o in mk.get("outcomes", []) if o.get("name") and o.get("price") is not None}
                    H, A = m.get(home), m.get(away)
                    D    = m.get("Draw") or m.get("Tie")
                    break
            if H or D or A: break
        rows.append({"sport_key":sk,"date":dt,"home_team":home,"away_team":away,
                     "home_odds_dec":H,"draw_odds_dec":D,"away_odds_dec":A})
    return rows

def main():
    if not API_KEY:
        write_empty("No THE_ODDS_API_KEY set")

    all_rows=[]
    for sk in SPORT_KEYS:
        try:
            all_rows += fetch_key(sk)
            time.sleep(0.5)  # be gentle
        except Exception as e:
            print(f"[WARN] fetch failed for {sk}: {e}")

    if not all_rows:
        write_empty("No odds returned from any sport key")

    out = pd.DataFrame(all_rows)
    # normalize date
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.tz_localize(None)
    # dedupe: keep the most recent row per (date, home, away)
    out.sort_values(["date","sport_key"], inplace=True)
    out = out.drop_duplicates(subset=["date","home_team","away_team"], keep="last")
    out.to_csv(OUT, index=False)
    print(f"[OK] wrote {OUT} rows={len(out)} across {len(SPORT_KEYS)} sport keys")

if __name__ == "__main__":
    main()