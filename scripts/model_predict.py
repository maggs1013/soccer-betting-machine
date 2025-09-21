# scripts/model_predict.py
# Simple, robust predictor using available odds.
# Avoids column mismatch errors.

import os
import pandas as pd
import numpy as np

DATA = "data"
UPCOMING = os.path.join(DATA, "UPCOMING_7D_enriched.csv")
OUT = os.path.join(DATA, "PREDICTIONS_7D.csv")

def main():
    if not os.path.exists(UPCOMING):
        print("[INFO] No upcoming file; writing empty predictions.")
        pd.DataFrame(columns=["date","home_team","away_team","pred_home","pred_draw","pred_away","kelly_home","kelly_draw","kelly_away"]).to_csv(OUT, index=False)
        return

    up = pd.read_csv(UPCOMING)
    if up.empty:
        print("[INFO] UPCOMING file empty; writing header-only predictions.")
        pd.DataFrame(columns=["date","home_team","away_team","pred_home","pred_draw","pred_away","kelly_home","kelly_draw","kelly_away"]).to_csv(OUT, index=False)
        return

    # Ensure required columns
    need = {"date","home_team","away_team","home_odds_dec","draw_odds_dec","away_odds_dec"}
    for c in need:
        if c not in up.columns:
            up[c] = np.nan

    # Convert odds to probabilities
    preds = []
    for _, r in up.iterrows():
        try:
            h, d, a = r["home_odds_dec"], r["draw_odds_dec"], r["away_odds_dec"]
            if pd.isna(h) or pd.isna(d) or pd.isna(a):
                preds.append((np.nan,np.nan,np.nan))
                continue
            inv = 1/float(h) + 1/float(d) + 1/float(a)
            ph, pd_, pa = (1/float(h))/inv, (1/float(d))/inv, (1/float(a))/inv
            preds.append((ph,pd_,pa))
        except Exception:
            preds.append((np.nan,np.nan,np.nan))

    up["pred_home"], up["pred_draw"], up["pred_away"] = zip(*preds)

    # Kelly criterion (simple version: 1x bankroll, fair odds assumption)
    def kelly(p, odds):
        if pd.isna(p) or pd.isna(odds): return 0.0
        b = float(odds) - 1.0
        return max((p*(b+1) - 1)/b, 0.0)

    up["kelly_home"] = [kelly(ph, o) for ph,o in zip(up["pred_home"], up["home_odds_dec"])]
    up["kelly_draw"] = [kelly(pd_, o) for pd_,o in zip(up["pred_draw"], up["draw_odds_dec"])]
    up["kelly_away"] = [kelly(pa, o) for pa,o in zip(up["pred_away"], up["away_odds_dec"])]

    keep = ["date","home_team","away_team","pred_home","pred_draw","pred_away","kelly_home","kelly_draw","kelly_away"]
    up[keep].to_csv(OUT, index=False)
    print(f"[OK] wrote {OUT} with {len(up)} rows")

if __name__ == "__main__":
    main()