#!/usr/bin/env python3
import os, glob, json
import pandas as pd
from datetime import datetime

DATA_DIR = "data"
RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
os.makedirs(RUN_DIR, exist_ok=True)
OUT = os.path.join(RUN_DIR, "FEATURE_DRIFT.csv")

def main():
    rows=[]
    for fn in glob.glob(os.path.join(DATA_DIR, "models_btts", "btts_model__*.job.json")):
        league = os.path.basename(fn).split("__",1)[-1].replace(".job.json","")
        try:
            meta = json.load(open(fn))
            coef = meta.get("coef_info")
            if coef is not None:
                rows.append({"league":league, "model":"BTTS", "coef_summary":str(coef)})
        except Exception:
            continue
    pd.DataFrame(rows).to_csv(OUT, index=False)
    print("FEATURE_DRIFT.csv written")

if __name__ == "__main__":
    main()