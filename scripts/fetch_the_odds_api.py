import os, sys, requests, pandas as pd
OUT="data/raw_theodds_fixtures.csv"; MANUAL="data/manual_odds.csv"
API=os.environ.get("THE_ODDS_API_KEY","").strip(); BASE="https://api.the-odds-api.com/v4"
SPORT=os.environ.get("ODDS_SPORT_KEY","").strip(); REGIONS=os.environ.get("ODDS_REGIONS","eu,uk,us,au").strip()
def write_empty(msg):
    pd.DataFrame(columns=["date","home_team","away_team","home_odds_dec","draw_odds_dec","away_odds_dec"]).to_csv(OUT,index=False)
    print("WARN:", msg, "→ wrote", OUT); sys.exit(0)
def use_manual():
    if not os.path.exists(MANUAL): return False
    df=pd.read_csv(MANUAL)
    need={"date","home_team","away_team","home_odds_dec","draw_odds_dec","away_odds_dec"}
    if need.issubset(df.columns) and len(df):
        df["date"]=pd.to_datetime(df["date"],errors="coerce").dt.tz_localize(None); df.to_csv(OUT,index=False)
        print("[OK] using manual odds", MANUAL, "→", OUT, len(df)); return True
    print("manual_odds.csv invalid → API fallback"); return False
def main():
    if use_manual(): return
    if not API: write_empty("No THE_ODDS_API_KEY; supply data/manual_odds.csv")
    def get(url,params=None): return requests.get(url,params=params or {},timeout=30)
    r=get(f"{BASE}/sports/",params={"apiKey":API})
    if r.status_code!=200: write_empty(f"/sports {r.status_code} {r.text[:100]}")
    sports=r.json(); sk=SPORT
    if not sk:
        cand=[s["key"] for s in sports if "soccer" in s["key"] and ("uefa" in s["key"] or "champ" in s["key"])]
        sk=cand[0] if cand else ( [s["key"] for s in sports if s["key"]=="soccer_epl"] or [None] )[0]
        if not sk: write_empty("No suitable sport key")
    r=get(f"{BASE}/sports/{sk}/odds",params={"apiKey":API,"regions":REGIONS,"markets":"h2h","oddsFormat":"decimal"})
    if r.status_code!=200: write_empty(f"/odds {r.status_code} {r.text[:100]}")
    rows=[]
    for g in r.json():
        home,away=g.get("home_team"),g.get("away_team"); commence=g.get("commence_time")
        H=D=A=None
        for bm in g.get("bookmakers",[]):
            for mk in bm.get("markets",[]):
                if mk.get("key")=="h2h":
                    m={o["name"]:float(o["price"]) for o in mk.get("outcomes",[]) if "name" in o and "price" in o}
                    H=m.get(home); A=m.get(away); D=m.get("Draw") or m.get("Tie"); break
            if H or D or A: break
        rows.append({"date":commence,"home_team":home,"away_team":away,"home_odds_dec":H,"draw_odds_dec":D,"away_odds_dec":A})
    pd.DataFrame(rows).assign(date=lambda d: pd.to_datetime(d["date"],errors="coerce").dt.tz_localize(None)).to_csv(OUT,index=False)
    print("[OK]", OUT, len(rows))
if __name__=="__main__": main()
