import os, sys, time, requests, pandas as pd
API_KEY=os.environ.get("FBR_API_KEY","").strip(); BASE="https://fbrapi.com"; DATA="data"
LEAGUE_IDS=[9,12,11,20,13]  # EPL, LaLiga, Serie A, Bundesliga, Ligue 1
os.makedirs(DATA, exist_ok=True)
def get(path,params=None):
    h={"X-API-Key":API_KEY} if API_KEY else {}
    r=requests.get(f"{BASE}{path}",params=params or {},headers=h,timeout=30)
    return r.json() if r.status_code==200 else None
def list_seasons(lid):
    js=get("/league-seasons",{"league_id":lid}); 
    if not js: return []
    d=js.get("data",js); 
    try: d=sorted(d,key=lambda x:x.get("season_id",0))
    except: pass
    return d
def standings(lid,season_id=None):
    p={"league_id":lid}; 
    if season_id: p["season_id"]=season_id
    js=get("/league-standings",p); 
    if not js: return []
    d=js.get("data",js); rows=[]
    for r in d:
        t=r.get("team") or r.get("team_name"); 
        if not t: continue
        rows.append({"league_id":lid,"season_id":season_id,"season":r.get("season") or r.get("year"),
                     "team":str(t).strip(),"xg":r.get("xg"),"xga":r.get("xga"),
                     "xgd":r.get("xgd"),"xgd_per90":r.get("xgd_per90") or r.get("xgd_90")})
    return rows
def to_df(rows,cols):
    if not rows: return pd.DataFrame(columns=cols)
    df=pd.DataFrame(rows); df["team"]=df["team"].str.replace(r"\s+\(.*\)$","",regex=True).str.strip(); return df
def main():
    if not API_KEY:
        print("[INFO] No FBR_API_KEY; writing empty xg files")
        empty=["league_id","season_id","season","team","xg","xga","xgd","xgd_per90"]
        for n in ["current","last","hybrid"]:
            pd.DataFrame(columns=(["team","league_id","xg_hybrid","xga_hybrid","xgd_hybrid","xgd90_hybrid"] if n=="hybrid" else empty)).to_csv(os.path.join(DATA,f"xg_metrics_{n}.csv"),index=False)
        sys.exit(0)
    cols=["league_id","season_id","season","team","xg","xga","xgd","xgd_per90"]
    cur,last=[],[]
    for lid in LEAGUE_IDS:
        ss=list_seasons(lid); 
        if not ss: continue
        cur.extend(standings(lid, ss[-1].get("season_id"))); time.sleep(3.2)
        if len(ss)>=2: last.extend(standings(lid, ss[-2].get("season_id"))); time.sleep(3.2)
    dfc, dfl = to_df(cur, cols), to_df(last, cols)
    dfc.to_csv(os.path.join(DATA,"xg_metrics_current.csv"),index=False)
    dfl.to_csv(os.path.join(DATA,"xg_metrics_last.csv"),index=False)
    def sel(df,prefix):
        return df.rename(columns={"xg":f"{prefix}_xg","xga":f"{prefix}_xga","xgd":f"{prefix}_xgd","xgd_per90":f"{prefix}_xgd90"})[["team","league_id",f"{prefix}_xg",f"{prefix}_xga",f"{prefix}_xgd",f"{prefix}_xgd90"]]
    hybrid = sel(dfc,"cur") if not dfc.empty else pd.DataFrame(columns=["team","league_id","cur_xg","cur_xga","cur_xgd","cur_xgd90"])
    if not dfl.empty: hybrid = hybrid.merge(sel(dfl,"last"), on=["team","league_id"], how="outer")
    else:
        for c in ["last_xg","last_xga","last_xgd","last_xgd90"]: hybrid[c]=None
    for c in ["cur_xg","cur_xga","cur_xgd","cur_xgd90","last_xg","last_xga","last_xgd","last_xgd90"]:
        hybrid[c]=pd.to_numeric(hybrid[c],errors="coerce")
    wcur,wlast=0.60,0.40
    def w(a,b): 
        if pd.notna(a) and pd.notna(b): return wcur*a+wlast*b
        if pd.notna(a): return a
        if pd.notna(b): return b
        return None
    out = pd.DataFrame({
        "team": hybrid["team"], "league_id": hybrid["league_id"],
        "xg_hybrid": [w(a,b) for a,b in zip(hybrid["cur_xg"], hybrid["last_xg"])],
        "xga_hybrid":[w(a,b) for a,b in zip(hybrid["cur_xga"], hybrid["last_xga"])],
        "xgd_hybrid":[w(a,b) for a,b in zip(hybrid["cur_xgd"], hybrid["last_xgd"])],
        "xgd90_hybrid":[w(a,b) for a,b in zip(hybrid["cur_xgd90"], hybrid["last_xgd90"])]
    })
    out.to_csv(os.path.join(DATA,"xg_metrics_hybrid.csv"),index=False)
    print("[OK] wrote data/xg_metrics_hybrid.csv", len(out))
if __name__=="__main__": main()
