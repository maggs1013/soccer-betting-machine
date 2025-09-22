#!/usr/bin/env python3
import os, math
import pandas as pd
from datetime import datetime
from math import exp

DATA_DIR = "data"
RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
os.makedirs(RUN_DIR, exist_ok=True)

MU_CSV = os.path.join(RUN_DIR, "GOALS_MU.csv")
TOTALS_OUT = os.path.join(RUN_DIR, "PREDICTIONS_TOTALS_7D.csv")
MAIN_LINE = 2.5

def pois_pmf(lmbd, k):
    return (lmbd**k * exp(-lmbd)) / math.factorial(k)

def prob_over_line(mu_home, mu_away, line=2.5, cap=10):
    # integer grid approximation
    p = 0.0
    for h in range(0, cap+1):
        for a in range(0, cap+1):
            s = h + a
            ph = pois_pmf(mu_home, h)
            pa = pois_pmf(mu_away, a)
            if s > line:
                p += ph*pa
    return min(max(p, 0.0), 1.0)

def main():
    mu = pd.read_csv(MU_CSV)
    mu["p_over"] = [prob_over_line(r.mu_home, r.mu_away, MAIN_LINE) for r in mu.itertuples()]
    mu["p_under"] = 1.0 - mu["p_over"]
    mu.rename(columns={"league":"league", "fixture_id":"fixture_id"}, inplace=True)
    mu.to_csv(TOTALS_OUT, index=False)
    print("Totals pricing written.")

if __name__ == "__main__":
    main()