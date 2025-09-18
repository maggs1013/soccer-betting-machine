# Reads HIST_matches.csv (for Elo), and UPCOMING_7D_enriched.csv (features/odds),
# outputs data/PREDICTIONS_7D.csv with Market+Elo(+xG) and Kelly (if odds provided).
import os, pandas as pd, numpy as np

DATA="data"
HIST=os.path.join(DATA,"HIST_matches.csv")
UP7 =os.path.join(DATA,"UPCOMING_7D_enriched.csv")
OUT =os.path.join(DATA,"PREDICTIONS_7D.csv")

if not (os.path.exists(HIST) and os.path.exists(UP7)):
    print("[WARN] missing HIST/UPCOMING_7D_enriched"); raise SystemExit

hist=pd.read_csv(HIST); up=pd.read_csv(UP7)

def implied(dec):
    try: d=float(dec); return 1.0/d if d>0 else np.nan
    except: return np.nan
def strip_vig(pH,pD,pA):
    s=np.nansum([pH,pD,pA]); 
    return (pH/s,pD/s,pA/s) if s and s>0 else (np.nan,np.nan,np.nan)

# Elo from HIST
ratings={t:1500.0 for t in pd.unique(pd.concat([hist["home_team"],hist["away_team"]]).dropna())}
for r in hist.itertuples(index=False):
    h,a=r.home_team,r.away_team
    if pd.isna(h) or pd.isna(a): continue
    Rh,Ra=ratings.get(h,1500.0),ratings.get(a,1500.0)
    Eh=1/(1+10**(-((Rh-Ra+60)/400)))
    hg=getattr(r,"home_goals",np.nan); ag=getattr(r,"away_goals",np.nan)
    if pd.isna(hg) or pd.isna(ag): continue
    score=1.0 if hg>ag else (0.5 if hg==ag else 0.0); K=20
    ratings[h]=Rh+K*(score-Eh); ratings[a]=Ra+K*((1.0-score)-(1.0-Eh))
def elo_pH(diff): return 1/(1+10**(-((diff+60)/400)))
def elo_pD(diff): return 0.18+0.10*np.exp(-abs(diff)/200.0)

up["elo_H"]=up["home_team"].map(ratings).fillna(1500.0)
up["elo_A"]=up["away_team"].map(ratings).fillna(1500.0)
up["elo_diff"]=up["elo_H"]-up["elo_A"]
up["pH_elo_core"]=up["elo_diff"].apply(elo_pH)
up["pD_elo"]=up["elo_diff"].apply(elo_pD)
up["pH_elo"]=(1-up["pD_elo"])*up["pH_elo_core"]; up["pA_elo"]=1-up["pH_elo"]-up["pD_elo"]

# xG nudge if hybrid present
def xg_nudge(row):
    pH,pD,pA=row["pH_elo"],row["pD_elo"],row["pA_elo"]
    hxg,axg=row.get("home_xgd90",np.nan),row.get("away_xgd90",np.nan)
    if pd.isna(hxg) or pd.isna(axg): return pH,pD,pA
    diff=hxg-axg; mag=0.0
    if abs(diff)>0.5: mag=min(0.02, 0.01+0.02*((abs(diff)-0.5)/1.0))
    if diff>0.5: pH+=mag; pA-=mag
    elif diff<-0.5: pH-=mag; pA+=mag
    s=pH+pD+pA; 
    return (pH/s,pD/s,pA/s) if s>0 else (row["pH_elo"],row["pD_elo"],row["pA_elo"])
up[["pH_model","pD_model","pA_model"]]=up.apply(xg_nudge,axis=1,result_type="expand")

# market blend + Kelly if odds provided
w_mkt,w_model=0.60,0.40
up["mkt_pH_raw"]=up["home_odds_dec"].apply(implied)
up["mkt_pD_raw"]=up["draw_odds_dec"].apply(implied)
up["mkt_pA_raw"]=up["away_odds_dec"].apply(implied)
up[["mkt_pH","mkt_pD","mkt_pA"]]=up.apply(lambda r: strip_vig(r["mkt_pH_raw"],r["mkt_pD_raw"],r["mkt_pA_raw"]),axis=1,result_type="expand")

up["pH_final"]=w_mkt*up["mkt_pH"].fillna(up["pH_model"])+w_model*up["pH_model"]
up["pD_final"]=w_mkt*up["mkt_pD"].fillna(up["pD_model"])+w_model*up["pD_model"]
up["pA_final"]=w_mkt*up["mkt_pA"].fillna(up["pA_model"])+w_model*up["pA_model"]
s=up["pH_final"]+up["pD_final"]+up["pA_final"]; up["pH_final"]/=s; up["pD_final"]/=s; up["pA_final"]/=s

def kelly(p, dec, cap=0.15):
    try: b=float(dec)-1.0; 
    except: return 0.0
    if b<=0 or pd.isna(p): return 0.0
    q=1-p; k=(b*p - q)/b; return max(0.0, min(k, cap))

up["kelly_H"]=up.apply(lambda r: kelly(r["pH_final"],r["home_odds_dec"]),axis=1)
up["kelly_D"]=up.apply(lambda r: kelly(r["pD_final"],r["draw_odds_dec"]),axis=1)
up["kelly_A"]=up.apply(lambda r: kelly(r["pA_final"],r["away_odds_dec"]),axis=1)
up["top_kelly"]=up[["kelly_H","kelly_D","kelly_A"]].max(axis=1)

cols=["date","home_team","away_team","home_odds_dec","draw_odds_dec","away_odds_dec",
      "pH_final","pD_final","pA_final","kelly_H","kelly_D","kelly_A",
      "home_xg","home_xga","home_xgd90","away_xg","away_xga","away_xgd90","top_kelly"]
up.sort_values(["date","top_kelly"],ascending=[True,False])[cols].to_csv(OUT,index=False)
print("[OK] wrote",OUT)
