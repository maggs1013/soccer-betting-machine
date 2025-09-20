# scripts/consolidate_odds.py
# Merge multiple odds CSVs into data/manual_odds.csv preferring FanDuel rows.
# Sources searched (optional): data/fanduel_odds.csv, data/oddsportal_export.csv

import os, pandas as pd

DATA="data"
OUT=os.path.join(DATA,"manual_odds.csv")
TEAM_MAP=os.path.join(DATA,"team_name_map.csv")
SOURCES=[os.path.join(DATA,"fanduel_odds.csv"), os.path.join(DATA,"oddsportal_export.csv")]

def load(p): return pd.read_csv(p) if os.path.exists(p) else pd.DataFrame()

def norm_cols(df):
    ren={"home_odds":"home_odds_dec","draw_odds":"draw_odds_dec","away_odds":"away_odds_dec",
         "Home":"home_odds_dec","Draw":"draw_odds_dec","Away":"away_odds_dec"}
    for k,v in ren.items():
        if k in df.columns and v not in df.columns: df[v]=df[k]
    return df

def load_team_map(path):
    if not os.path.exists(path): return {}
    tm=pd.read_csv(path)
    if "raw" not in tm.columns or "canonical" not in tm.columns: return {}
    tm=tm.dropna(subset=["raw","canonical"])
    return {str(r.raw).strip(): str(r.canonical).strip() for _,r in tm.iterrows()}

def apply_team_map(series, name_map):
    return series.apply(lambda x: name_map.get(str(x).strip(), str(x).strip()) if pd.notna(x) else x)

# Gather frames
frames=[]; name_map=load_team_map(TEAM_MAP)
for src in SOURCES:
    df=load(src); 
    if df.empty: continue
    df=norm_cols(df)
    # normalize teams
    if "home_team" in df.columns: df["home_team"]=apply_team_map(df["home_team"], name_map)
    if "away_team" in df.columns: df["away_team"]=apply_team_map(df["away_team"], name_map)
    df["source"]=os.path.basename(src) if "source" not in df.columns else df["source"]
    need=["date","home_team","away_team","home_odds_dec","draw_odds_dec","away_odds_dec","source"]
    miss=[c for c in need if c not in df.columns]
    if miss:
        print(f"[WARN] {src} missing {miss}; skip")
        continue
    # parse date to ISO
    df["date"]=pd.to_datetime(df["date"],errors="coerce").dt.tz_localize(None)
    frames.append(df[need])

if not frames:
    print("[INFO] No external odds sources found; please provide data/manual_odds.csv yourself.")
else:
    allodds=pd.concat(frames, ignore_index=True).dropna(subset=["home_team","away_team"])
    # Prefer FanDuel rows if present
    def prefer_fanduel(g):
        fd=g[g["source"].str.contains("fanduel",case=False,na=False)]
        return fd.iloc[0] if len(fd) else g.iloc[0]
    merged=(allodds.groupby(["date","home_team","away_team"],as_index=False)
                   .apply(prefer_fanduel).droplevel(0).reset_index(drop=True))
    merged[["date","home_team","away_team","home_odds_dec","draw_odds_dec","away_odds_dec"]].to_csv(OUT,index=False)
    print(f"[OK] wrote {OUT} with {len(merged)} rows")