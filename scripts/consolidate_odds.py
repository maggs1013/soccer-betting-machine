import os, pandas as pd
DATA="data"; OUT=os.path.join(DATA,"manual_odds.csv")
SOURCES=[os.path.join(DATA,"fanduel_odds.csv"), os.path.join(DATA,"oddsportal_export.csv")]

def load(p): return pd.read_csv(p) if os.path.exists(p) else pd.DataFrame()
def norm(df):
    ren={"home_odds":"home_odds_dec","draw_odds":"draw_odds_dec","away_odds":"away_odds_dec",
         "Home":"home_odds_dec","Draw":"draw_odds_dec","Away":"away_odds_dec"}
    for k,v in ren.items():
        if k in df.columns and v not in df.columns: df[v]=df[k]
    return df

frames=[]
for s in SOURCES:
    df=load(s); 
    if df.empty: continue
    df=norm(df); df["source"]=os.path.basename(s)
    frames.append(df[["date","home_team","away_team","home_odds_dec","draw_odds_dec","away_odds_dec","source"]])

if not frames:
    print("[INFO] no external odds sources found; provide data/manual_odds.csv yourself")
else:
    allodds=pd.concat(frames, ignore_index=True).dropna(subset=["home_team","away_team"])
    def prefer_fd(g):
        fd = g[g["source"].str.contains("fanduel",case=False,na=False)]
        return fd.iloc[0] if len(fd) else g.iloc[0]
    f = allodds.groupby(["date","home_team","away_team"],as_index=False).apply(prefer_fd).droplevel(0).reset_index(drop=True)
    f[["date","home_team","away_team","home_odds_dec","draw_odds_dec","away_odds_dec"]].to_csv(OUT,index=False)
    print("[OK] wrote", OUT, len(f))
