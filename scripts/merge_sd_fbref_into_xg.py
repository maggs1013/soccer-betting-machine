import os, pandas as pd
DATA="data"; HYB=os.path.join(DATA,"xg_metrics_hybrid.csv"); FBRS=os.path.join(DATA,"sd_fbref_team_stats.csv")
if not os.path.exists(HYB) or not os.path.exists(FBRS):
    print("[INFO] hybrid or sd_fbref missing; skip merge")
else:
    hyb=pd.read_csv(HYB); fb=pd.read_csv(FBRS)
    # try to detect per-season totals; use simple per-90 proxy if matches column available
    if "matches" in fb.columns:
        fb["xg_for_p90_fbref"]=pd.to_numeric(fb.get("xg_for_fbref",None),errors="coerce")/fb["matches"]
        fb["xg_against_p90_fbref"]=pd.to_numeric(fb.get("xg_against_fbref",None),errors="coerce")/fb["matches"]
    else:
        fb["xg_for_p90_fbref"]=pd.to_numeric(fb.get("xg_for_fbref",None),errors="coerce")
        fb["xg_against_p90_fbref"]=pd.to_numeric(fb.get("xg_against_fbref",None),errors="coerce")
    fb_small=fb[["team","xg_for_p90_fbref","xg_against_p90_fbref"]].dropna()
    m=hyb.merge(fb_small,on="team",how="left")
    m["xg_hybrid"]=m["xg_hybrid"].fillna(m["xg_for_p90_fbref"])
    m["xga_hybrid"]=m["xga_hybrid"].fillna(m["xg_against_p90_fbref"])
    m["xgd90_hybrid"]=m["xgd90_hybrid"].fillna(m["xg_for_p90_fbref"]-m["xg_against_p90_fbref"])
    m.to_csv(HYB,index=False); print("[OK] merged FBref fallback into",HYB)
