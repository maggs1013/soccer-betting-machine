# scripts/coverage_trends.py
# Append coverage metrics over time for monitoring data health.
# Reads: data/DATA_QUALITY_REPORT.csv
# Writes (appends): data/COVERAGE_TRENDS.csv

import os, pandas as pd
from datetime import datetime

DATA="data"
SRC = os.path.join(DATA,"DATA_QUALITY_REPORT.csv")
OUT = os.path.join(DATA,"COVERAGE_TRENDS.csv")

def main():
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    if not os.path.exists(SRC):
        pd.DataFrame(columns=["run_time","metric","count","total","percent"]).to_csv(OUT,index=False)
        print(f"[WARN] Missing {SRC}; wrote empty {OUT}")
        return
    dqr=pd.read_csv(SRC)
    if dqr.empty:
        pd.DataFrame(columns=["run_time","metric","count","total","percent"]).to_csv(OUT,index=False)
        print(f"[WARN] Empty {SRC}; wrote empty {OUT}")
        return
    dqr["run_time"]=now
    cols=["run_time","metric","count","total","percent"]
    new=dqr[cols].copy()
    if os.path.exists(OUT):
        try:
            old=pd.read_csv(OUT)
            allp=pd.concat([old,new],ignore_index=True)
        except:
            allp=new
    else:
        allp=new
    allp.to_csv(OUT,index=False)
    print(f"[OK] wrote {OUT} rows={len(allp)}")

if __name__ == "__main__":
    main()