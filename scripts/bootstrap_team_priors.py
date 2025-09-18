import os, pandas as pd
DATA="data"; IN=os.path.join(DATA,"xg_metrics_hybrid.csv"); OUT=os.path.join(DATA,"teams_master.csv")
def clamp(v,lo,hi):
    try: v=float(v)
    except: return (lo+hi)/2
    return max(lo,min(hi,v))
if not os.path.exists(IN):
    pd.DataFrame(columns=["team","gk_rating","setpiece_rating","crowd_index"]).to_csv(OUT,index=False)
    print("[WARN] wrote empty",OUT); 
else:
    df=pd.read_csv(IN); rows=[]
    for r in df.itertuples(index=False):
        team=getattr(r,"team",None); xgd90=getattr(r,"xgd90_hybrid",None); xga=getattr(r,"xga_hybrid",None)
        setp=0.55+(0.10 if (pd.notna(xgd90) and xgd90>0) else -0.10 if (pd.notna(xgd90) and xgd90<0) else 0.0)
        gk=0.80-0.15*max(0.0,(xga/34.0) if pd.notna(xga) else 0.0)
        rows.append({"team":team,"gk_rating":clamp(gk,0.55,0.90),"setpiece_rating":clamp(setp,0.50,0.85),"crowd_index":0.70})
    pd.DataFrame(rows).drop_duplicates("team").to_csv(OUT,index=False); print("[OK] wrote",OUT)
