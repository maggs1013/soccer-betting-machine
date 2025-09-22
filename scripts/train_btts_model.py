#!/usr/bin/env python3
import os, json
import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.calibration import calibration_curve
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import log_loss, brier_score_loss
from datetime import datetime

DATA_DIR = "data"
RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
os.makedirs(RUN_DIR, exist_ok=True)

INPUT_ENRICHED = os.path.join(DATA_DIR, "enriched_fixtures.csv")  # produced earlier in your pipeline
BTTS_LABEL_COL = "btts_y"  # 1 if both teams scored, 0 otherwise (historical rows only)
LEAGUE_COL = "league"
SPLIT_COL = "is_train"  # boolean or 0/1 you already use in backtests

FEATURES = [
    # keep in sync with your engineer_variables/build_rolling_features
    "spi_home", "spi_away", "form_ppg_home", "form_ppg_away",
    "xg_for_home_5", "xg_against_home_5", "xg_for_away_5", "xg_against_away_5",
    "finishing_luck_home_5", "finishing_luck_away_5",
    "keeper_dependence_home_5", "keeper_dependence_away_5",
    "set_piece_share_home_5", "set_piece_share_away_5",
    "rest_days_home", "rest_days_away"
]

MODEL_DIR = os.path.join(DATA_DIR, "models_btts")
os.makedirs(MODEL_DIR, exist_ok=True)

def fit_per_league(df):
    rows = []
    for lg, g in df.groupby(LEAGUE_COL):
        train = g[g[SPLIT_COL] == 1].dropna(subset=FEATURES+[BTTS_LABEL_COL])
        if len(train) < 300 or train[BTTS_LABEL_COL].nunique() < 2:
            continue
        X = train[FEATURES].values
        y = train[BTTS_LABEL_COL].values
        pipe = Pipeline([
            ("ss", StandardScaler()),
            ("lr", LogisticRegression(max_iter=200))
        ])
        pipe.fit(X, y)
        # Eval on train as quick fit report; proper OOS is in backtest module
        p = pipe.predict_proba(X)[:,1]
        ll = log_loss(y, p, labels=[0,1])
        br = brier_score_loss(y, p)
        meta = {"league": lg, "n": int(len(train)), "logloss": float(ll), "brier": float(br)}
        rows.append(meta)
        # persist
        fn = os.path.join(MODEL_DIR, f"btts_model__{lg}.job.json")
        with open(fn, "w") as f:
            json.dump({"features": FEATURES, "coef_info": getattr(pipe.named_steps["lr"], "coef_", []).tolist()}, f)
        # sklearn Pipeline is not JSON; persist via joblib
        import joblib
        joblib.dump(pipe, os.path.join(MODEL_DIR, f"btts_model__{lg}.joblib"))
    return pd.DataFrame(rows)

def main():
    df = pd.read_csv(INPUT_ENRICHED)
    report = fit_per_league(df)
    report.to_csv(os.path.join(RUN_DIR, "BTTS_TRAIN_REPORT.csv"), index=False)
    print("BTTS training complete:", len(report), "leagues")

if __name__ == "__main__":
    main()