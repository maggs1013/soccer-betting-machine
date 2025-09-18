import os, pandas as pd
from io import StringIO
import requests

OUT = "data/raw_football_data.csv"
os.makedirs("data", exist_ok=True)

URLS_2425 = [
    "https://www.football-data.co.uk/mmz4281/2425/E0.csv",
    "https://www.football-data.co.uk/mmz4281/2425/D1.csv",
    "https://www.football-data.co.uk/mmz4281/2425/I1.csv",
    "https://www.football-data.co.uk/mmz4281/2425/SP1.csv",
    "https://www.football-data.co.uk/mmz4281/2425/F1.csv",
]
URLS_2324 = [
    "https://www.football-data.co.uk/mmz4281/2324/E0.csv",
    "https://www.football-data.co.uk/mmz4281/2324/D1.csv",
    "https://www.football-data.co.uk/mmz4281/2324/I1.csv",
    "https://www.football-data.co.uk/mmz4281/2324/SP1.csv",
    "https://www.football-data.co.uk/mmz4281/2324/F1.csv",
]
URLS = URLS_2425 + URLS_2324

def download_csv(url):
    r = requests.get(url, timeout=60); r.raise_for_status()
    return pd.read_csv(StringIO(r.text))

def normalize(df):
    cols = df.columns.str.upper(); df.columns = cols
    def pick(cands): 
        for c in cands:
            if c in df.columns: return c
        return None
    k = {
        "date":"DATE","home_team":"HOMETEAM","away_team":"AWAYTEAM",
        "home_goals":"FTHG","away_goals":"FTAG",
        "home_odds_dec": pick(["B365H","PSH","WHH","IWH"]),
        "draw_odds_dec": pick(["B365D","PSD","WHD","IWD"]),
        "away_odds_dec": pick(["B365A","PSA","WHA","IWA"]),
    }
    out = {k1:(df[k2] if k2 in df.columns else pd.Series([None]*len(df))) for k1,k2 in k.items()}
    o = pd.DataFrame(out).dropna(subset=["date","home_team","away_team"])
    o["date"] = pd.to_datetime(o["date"], dayfirst=True, errors="coerce")
    o = o.dropna(subset=["date"]).sort_values("date")
    # enrich defaults; enrichment will override later
    o["home_rest_days"]=4; o["away_rest_days"]=4
    o["home_travel_km"]=200; o["away_travel_km"]=200
    o["home_injury_index"]=0.3; o["away_injury_index"]=0.3
    o["home_gk_rating"]=0.6; o["away_gk_rating"]=0.6
    o["home_setpiece_rating"]=0.6; o["away_setpiece_rating"]=0.6
    o["ref_pen_rate"]=0.30; o["crowd_index"]=0.7
    return o

frames=[]
for u in URLS:
    try: frames.append(normalize(download_csv(u)))
    except Exception as e: print("Skipped:", u, e)

if frames:
    pd.concat(frames, ignore_index=True).to_csv(OUT, index=False)
    print("[OK]", OUT)
else:
    pd.DataFrame(columns=["date","home_team","away_team","home_goals","away_goals",
                          "home_odds_dec","draw_odds_dec","away_odds_dec",
                          "home_rest_days","away_rest_days","home_travel_km","away_travel_km",
                          "home_injury_index","away_injury_index","home_gk_rating","away_gk_rating",
                          "home_setpiece_rating","away_setpiece_rating","ref_pen_rate","crowd_index"]).to_csv(OUT, index=False)
    print("[WARN] wrote empty", OUT)
