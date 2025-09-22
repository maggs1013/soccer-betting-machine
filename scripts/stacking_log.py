#!/usr/bin/env python3
import os
import pandas as pd
from datetime import datetime

RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
os.makedirs(RUN_DIR, exist_ok=True)
OUT = os.path.join(RUN_DIR, "STACKING_LOG.csv")

def main():
    # placeholder: write chosen weights for BTTS stack if/when you add totals-derived BTTS
    pd.DataFrame([{"market":"BTTS","league":"GLOBAL","w_direct":0.7,"w_from_totals":0.3}]).to_csv(OUT, index=False)
    print("STACKING_LOG.csv written")

if __name__ == "__main__":
    main()