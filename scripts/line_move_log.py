#!/usr/bin/env python3
import os
import pandas as pd
from datetime import datetime

DATA_DIR = "data"
RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
os.makedirs(RUN_DIR, exist_ok=True)

MORNING = os.path.join(DATA_DIR, "odds_snapshot_morning.csv")
TMINUS  = os.path.join(DATA_DIR, "odds_snapshot_tminus60.csv")
OUT = os.path.join(RUN_DIR, "LINE_MOVE_LOG.csv")

def main():
    if not (os.path.exists(MORNING) and os.path.exists(TMINUS)):
        pd.DataFrame(columns=["fixture_id","league","delta_oddsH","delta_oddsA","delta_total"]).to_csv(OUT, index=False)
        print("No snapshots; LINE_MOVE_LOG empty.")
        return
    a = pd.read_csv(MORNING)
    b = pd.read_csv(TMINUS)
    cols = ["fixture_id","league","oddsH","oddsA","total_line","odds_over"]
    m = a[cols].merge(b[cols], on=["fixture_id","league"], suffixes=("_am","_tm"))
    m["delta_oddsH"] = m["oddsH_tm"] - m["oddsH_am"]
    m["delta_oddsA"] = m["oddsA_tm"] - m["oddsA_am"]
    m["delta_total"] = m["odds_over_tm"] - m["odds_over_am"]
    out = m[["fixture_id","league","delta_oddsH","delta_oddsA","delta_total"]]
    out.to_csv(OUT, index=False)
    print("LINE_MOVE_LOG.csv written")

if __name__ == "__main__":
    main()