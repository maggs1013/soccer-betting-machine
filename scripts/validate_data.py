import os, pandas as pd
DATA="data"
def show(p,lab):
    if not os.path.exists(p): print("[WARN] missing",lab); return None
    df=pd.read_csv(p)
    print(f"\n==== {lab} ({len(df)}) ===="); print("Cols:", list(df.columns)[:20]); print(df.head(3)); return df
show(os.path.join(DATA,"HIST_matches.csv"),"HIST_matches.csv")
show(os.path.join(DATA,"UPCOMING_fixtures.csv"),"UPCOMING_fixtures.csv")
show(os.path.join(DATA,"UPCOMING_7D_enriched.csv"),"UPCOMING_7D_enriched.csv")
show(os.path.join(DATA,"xg_metrics_hybrid.csv"),"xg_metrics_hybrid.csv")
