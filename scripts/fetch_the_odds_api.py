# scripts/fetch_the_odds_api.py
# Odds API only (no manual override). Writes data/raw_theodds_fixtures.csv (or empty with headers).

import os, sys, requests, pandas as pd

DATA = "data"
OUT  = os.path.join(DATA, "raw_theodds_fixtures.csv")

API_KEY = os.environ.get("THE_ODDS_API_KEY", "").strip()
REGIONS = os.environ.get("ODDS_REGIONS", "eu,uk,us,au").strip()
MARKETS = os.environ.get("ODDS_MARKETS", "h2h").strip()
BASE    = "https://api.the-odds-api.com/v4"
SPORT   = os.environ.get("ODDS_SPORT_KEY", "").strip()

def write_empty(msg):
    pd.DataFrame(columns=["date","home_team","away_team","home_odds_dec","draw_odds_dec","away_odds_dec"]).to_csv(OUT, index=False)
    print("INFO:", msg, "| wrote empty", OUT)
    sys.exit(0)

def pick_sport_key():
    r = requests.get(f"{BASE}/sports/", params={"apiKey": API_KEY}, timeout=30)
    if r.status_code!=200: return ""
    sports = r.json()
    cands  = [s.get("key","") for s in sports if "soccer" in s.get("key","") and ("uefa" in s.get("key","") or "champ" in s.get("key",""))]
    if cands: return cands[0]
    epl = [s.get("key","") for s in sports if s.get("key","")=="soccer_epl"]
    return epl[0] if epl else ""

def main():
    if not API_KEY:
        write_empty("No THE_ODDS_API_KEY set")

    sk = SPORT or pick_sport_key()
    if not sk:
        write_empty("No suitable sport key found (set ODDS_SPORT_KEY or use available list)")

    params = {"apiKey": API_KEY, "regions": REGIONS, "markets": MARKETS, "oddsFormat": "decimal"}
    url = f"{BASE}/sports/{sk}/odds"
    r = requests.get(url, params=params, timeout=60)
    if r.status_code!=200:
        write_empty(f"Odds API error {r.status_code}: {r.text[:120]}")

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
        rows.append({"date":dt,"home_team":home,"away_team":away,"home_odds_dec":H,"draw_odds_dec":D,"away_odds_dec":A})

    out = pd.DataFrame(rows)
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.tz_localize(None)
    out.to_csv(OUT, index=False)
    print(f"[OK] wrote {OUT} rows={len(out)} (sport={sk}, regions={REGIONS})")

if __name__ == "__main__":
    main()