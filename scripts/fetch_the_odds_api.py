# scripts/fetch_the_odds_api.py
# Robust odds fetcher:
# 1) If data/manual_odds.csv exists and is NON-EMPTY with correct headers → use it.
# 2) Else, try The Odds API if THE_ODDS_API_KEY is set.
# 3) Else, write an EMPTY upcoming file (safe, CI continues).

import os
import sys
import pandas as pd
from pandas.errors import EmptyDataError
import requests

DATA_DIR = "data"
MANUAL = os.path.join(DATA_DIR, "manual_odds.csv")
OUT_UPCOMING = os.path.join(DATA_DIR, "raw_theodds_fixtures.csv")

API_KEY = os.environ.get("THE_ODDS_API_KEY", "").strip()
SPORT = os.environ.get("ODDS_SPORT_KEY", "").strip()          # optional
REGIONS = os.environ.get("ODDS_REGIONS", "eu,uk,us,au").strip()
MARKETS = os.environ.get("ODDS_MARKETS", "h2h").strip()
BASE = "https://api.the-odds-api.com/v4"

NEEDED_COLS = {"date","home_team","away_team",
               "home_odds_dec","draw_odds_dec","away_odds_dec"}

def write_empty_and_exit(msg: str):
    """Write an empty but valid CSV so downstream steps continue; exit 0 (success)."""
    pd.DataFrame(columns=["date","home_team","away_team",
                          "home_odds_dec","draw_odds_dec","away_odds_dec"]).to_csv(OUT_UPCOMING, index=False)
    print("INFO:", msg)
    print("INFO: wrote empty", OUT_UPCOMING)
    sys.exit(0)

def use_manual_if_valid() -> bool:
    """Return True if we used manual_odds.csv to write OUT_UPCOMING."""
    if not os.path.exists(MANUAL):
        return False
    try:
        df = pd.read_csv(MANUAL)
    except EmptyDataError:
        print("[INFO] manual_odds.csv exists but is EMPTY; skipping manual and trying API.")
        return False
    except Exception as e:
        print("[INFO] manual_odds.csv read error:", e, "→ skipping manual and trying API.")
        return False

    cols = set(df.columns)
    if NEEDED_COLS.issubset(cols) and len(df) > 0:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)
        df.to_csv(OUT_UPCOMING, index=False)
        print(f"[OK] using manual odds {MANUAL} → {OUT_UPCOMING} ({len(df)} rows)")
        return True
    elif NEEDED_COLS.issubset(cols) and len(df) == 0:
        print("[INFO] manual_odds.csv has header but NO rows; skipping manual and trying API.")
        return False
    else:
        missing = list(NEEDED_COLS - cols)
        print(f"[INFO] manual_odds.csv MISSING required columns: {missing}; skipping manual and trying API.")
        return False

def choose_sport_key() -> str:
    """Pick a sport key for Odds API if none provided; prefer UCL-like → fallback EPL."""
    r = requests.get(f"{BASE}/sports/", params={"apiKey": API_KEY}, timeout=30)
    if r.status_code != 200:
        return ""
    sports = r.json()
    # try UCL-like keys first
    cands = [s.get("key","") for s in sports if "soccer" in s.get("key","") and ("uefa" in s.get("key","") or "champ" in s.get("key",""))]
    if cands:
        return cands[0]
    # fallback: EPL or another common league
    for key in ("soccer_epl", "soccer_uefa_europa_league"):
        if any(s.get("key") == key for s in sports):
            return key
    return ""

def fetch_odds_from_api():
    """Try to fetch upcoming H2H odds from The Odds API; write OUT_UPCOMING (or empty)."""
    sk = SPORT or choose_sport_key()
    if not sk:
        write_empty_and_exit("No usable sport key found (set ODDS_SPORT_KEY or provide manual odds).")

    params = {"apiKey": API_KEY, "regions": REGIONS, "markets": MARKETS, "oddsFormat": "decimal"}
    url = f"{BASE}/sports/{sk}/odds"

    r = requests.get(url, params=params, timeout=60)
    if r.status_code != 200:
        write_empty_and_exit(f"Odds API request failed {r.status_code}: {r.text[:120]}")

    js = r.json()
    rows = []
    for game in js:
        home, away = game.get("home_team"), game.get("away_team")
        dt = game.get("commence_time")
        H = D = A = None
        for bm in game.get("bookmakers", []):
            for mk in bm.get("markets", []):
                if mk.get("key") == "h2h":
                    m = {o.get("name"): float(o.get("price")) for o in mk.get("outcomes", []) if o.get("name") and o.get("price") is not None}
                    H = m.get(home)
                    A = m.get(away)
                    D = m.get("Draw") or m.get("Tie")
                    break
            if H or D or A:
                break
        rows.append({"date": dt, "home_team": home, "away_team": away,
                     "home_odds_dec": H, "draw_odds_dec": D, "away_odds_dec": A})

    out = pd.DataFrame(rows)
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.tz_localize(None)
    out.to_csv(OUT_UPCOMING, index=False)
    print(f"[OK] wrote {OUT_UPCOMING} ({len(out)} rows) from The Odds API (sport={sk}, regions={REGIONS})")

def main():
    # 1) Manual override if present and valid
    if use_manual_if_valid():
        return
    # 2) Else try API if key is provided
    if API_KEY:
        fetch_odds_from_api()
        return
    # 3) Else write empty and continue (pipeline will still run)
    write_empty_and_exit("No THE_ODDS_API_KEY and manual_odds.csv unusable; writing empty upcoming file.")

if __name__ == "__main__":
    main()