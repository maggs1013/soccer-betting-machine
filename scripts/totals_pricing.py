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
    if lmbd <= 0:
        return 0.0
    try:
        return (lmbd**k * exp(-lmbd)) / math.factorial(k)
    except Exception:
        return 0.0

def prob_over_line(mu_home, mu_away, line=2.5, cap=10):
    # integer grid approximation
    p = 0.0
    for h in range(0, cap+1):
        ph = pois_pmf(mu_home, h)
        if ph == 0: 
            continue
        for a in range(0, cap+1):
            s = h + a
            if s > line:
                p += ph * pois_pmf(mu_away, a)
    return max(0.0, min(1.0, p))

def main():
    if not os.path.exists(MU_CSV):
        pd.DataFrame(columns=["fixture_id","league","p_over","p_under"]).to_csv(TOTALS_OUT, index=False)
        print(f"[WARN] {MU_CSV} missing; wrote empty {TOTALS_OUT}")
        return

    mu = pd.read_csv(MU_CSV)
    if mu.empty or not {"fixture_id","league","mu_home","mu_away"}.issubset(mu.columns):
        pd.DataFrame(columns=["fixture_id","league","p_over","p_under"]).to_csv(TOTALS_OUT, index=False)
        print(f"[WARN] GOALS_MU.csv invalid/empty; wrote empty {TOTALS_OUT}")
        return

    mu["mu_home"] = mu["mu_home"].fillna(1.35).clip(lower=0.2, upper=3.5)
    mu["mu_away"] = mu["mu_away"].fillna(1.25).clip(lower=0.2, upper=3.5)

    mu["p_over"] = [prob_over_line(r.mu_home, r.mu_away, MAIN_LINE) for r in mu.itertuples()]
    mu["p_under"] = 1.0 - mu["p_over"]
    out = mu[["fixture_id","league","p_over","p_under"]].copy()
    out.to_csv(TOTALS_OUT, index=False)
    print(f"[OK] Totals pricing written → {TOTALS_OUT} rows={len(out)}")

if __name__ == "__main__":
    main()