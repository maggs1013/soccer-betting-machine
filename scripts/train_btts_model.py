#!/usr/bin/env python3
"""
train_btts_model.py
Train BTTS (Both Teams To Score) per-league models from historical matches.

Reads (data/):
  HIST_matches.csv                  (date, home_team, away_team, home_goals, away_goals, league?)
  xg_metrics_hybrid.csv             (team, xg_hybrid, xga_hybrid, xgd90_hybrid)
  team_statsbomb_features.csv       (team, xa_sb, psxg_minus_goals_sb, setpiece_xg_sb, openplay_xg_sb)
  sd_538_spi.csv                    (team/squad/team_name, OFF/DEF columns)
  team_form_features.csv            (team, last5_ppg, last5_xgpg, last5_xgapg, last10_ppg, last10_xgpg, last10_xgapg)

Writes:
  runs/YYYY-MM-DD/BTTS_TRAIN_REPORT.csv
  data/models_btts/btts_model__{league}.joblib
  data/models_btts/btts_model__{league}.job.json
"""

import os, json
import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, brier_score_loss
import joblib

DATA = "data"
RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
os.makedirs(RUN_DIR, exist_ok=True)
MODEL_DIR = os.path.join(DATA, "models_btts")
os.makedirs(MODEL_DIR, exist_ok=True)

HIST = os.path.join(DATA, "HIST_matches.csv")
HYB  = os.path.join(DATA, "xg_metrics_hybrid.csv")
SBF  = os.path.join(DATA, "team_statsbomb_features.csv")
SPI  = os.path.join(DATA, "sd_538_spi.csv")
FORM = os.path.join(DATA, "team_form_features.csv")

LEAGUE_COL = "league"

# Feature names we will build for BOTH home_ and away_ (-> home_*, away_*)
BASE_FEATS = [
    "xg_hybrid","xga_hybrid","xgd90_hybrid",
    "xa_sb","psxg_minus_goals_sb","setpiece_share",
    "spi_off","spi_def",
    "last5_ppg","last5_xgpg","last5_xgapg",
    "last10_ppg","last10_xgpg","last10_xgapg",
]

def safe_read(path, cols=None):
    if not os.path.exists(path):
        return pd.DataFrame(columns=cols or [])
    try:
        df = pd.read_csv(path)
    except Exception:
        return pd.DataFrame(columns=cols or [])
    if cols:
        for c in cols:
            if c not in df.columns:
                df[c] = np.nan
    return df

def build_team_vectors():
    hyb = safe_read(HYB, ["team","xg_hybrid","xga_hybrid","xgd90_hybrid"])
    sbf = safe_read(SBF, ["team","xa_sb","psxg_minus_goals_sb","setpiece_xg_sb","openplay_xg_sb"])
    spi = safe_read(SPI)
    form = safe_read(FORM, [
        "team","last5_ppg","last5_xgpg","last5_xgapg","last10_ppg","last10_xgpg","last10_xgapg"
    ])

    # SPI: find team/off/def
    spi_cols = {c.lower(): c for c in spi.columns}
    if "team" not in spi.columns:
        if "squad" in spi.columns: spi = spi.rename(columns={"squad":"team"})
        elif "team_name" in spi.columns: spi = spi.rename(columns={"team_name":"team"})
    off_col = spi_cols.get("off") or spi_cols.get("offense")
    def_col = spi_cols.get("def")  or spi_cols.get("defense")
    spi_small = pd.DataFrame(columns=["team","spi_off","spi_def"])
    if "team" in spi.columns and off_col and def_col:
        spi_small = spi.groupby("team", as_index=False)[[off_col,def_col]].mean()
        spi_small = spi_small.rename(columns={off_col:"spi_off", def_col:"spi_def"})

    # base team list
    teamvec = pd.DataFrame({
        "team": pd.unique(pd.concat([hyb["team"], sbf["team"], spi_small["team"], form["team"]], ignore_index=True).dropna())
    })
    if teamvec.empty:
        return teamvec  # empty -> caller handles

    teamvec = teamvec.merge(hyb, on="team", how="left")

    if not sbf.empty:
        sbf2 = sbf.groupby("team", as_index=False).agg({
            "xa_sb":"mean", "psxg_minus_goals_sb":"mean",
            "setpiece_xg_sb":"mean", "openplay_xg_sb":"mean"
        })
        tot = (sbf2["setpiece_xg_sb"] + sbf2["openplay_xg_sb"]).replace(0,np.nan)
        sbf2["setpiece_share"] = (sbf2["setpiece_xg_sb"] / tot).fillna(0.0)
        teamvec = teamvec.merge(sbf2[["team","xa_sb","psxg_minus_goals_sb","setpiece_share"]], on="team", how="left")

    if not spi_small.empty:
        teamvec = teamvec.merge(spi_small, on="team", how="left")

    if not form.empty:
        teamvec = teamvec.merge(form, on="team", how="left")

    # Ensure all feature columns exist
    for c in BASE_FEATS:
        if c not in teamvec.columns:
            teamvec[c] = np.nan
    return teamvec

def build_training_frame(hist, teamvec):
    # label
    hist = hist.copy()
    if LEAGUE_COL not in hist.columns:
        hist[LEAGUE_COL] = "GLOBAL"
    # BTTS = 1 if both teams scored
    hist["btts_y"] = ((hist["home_goals"] > 0) & (hist["away_goals"] > 0)).astype(int)
    # features
    rows = []
    for r in hist.itertuples(index=False):
        ht, at, lg = r.home_team, r.away_team, getattr(r, LEAGUE_COL, "GLOBAL")
        hv = teamvec[teamvec["team"] == ht].head(1)
        av = teamvec[teamvec["team"] == at].head(1)
        if hv.empty or av.empty:
            continue
        row = {"league": lg, "btts_y": int(getattr(r, "btts_y"))}
        for c in BASE_FEATS:
            row[f"home_{c}"] = hv[c].iloc[0]
            row[f"away_{c}"] = av[c].iloc[0]
        rows.append(row)
    return pd.DataFrame(rows)

def train_per_league(df):
    feat_cols = [f"home_{c}" for c in BASE_FEATS] + [f"away_{c}" for c in BASE_FEATS]
    report_rows = []
    for lg, g in df.groupby("league"):
        # impute + scale + LR in one pipeline
        pipe = Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            ("lr", LogisticRegression(max_iter=400))
        ])
        # need at least some positives/negatives
        if g["btts_y"].nunique() < 2 or len(g) < 300:
            continue
        X = g[feat_cols].values
        y = g["btts_y"].values
        pipe.fit(X, y)
        # quick in-sample diagnostics
        p = pipe.predict_proba(X)[:,1]
        try:
            ll = log_loss(y, p, labels=[0,1])
            br = brier_score_loss(y, p)
        except Exception:
            ll, br = np.nan, np.nan

        report_rows.append({"league": lg, "n": int(len(g)), "logloss": float(ll), "brier": float(br)})

        # persist model + meta
        joblib.dump(pipe, os.path.join(MODEL_DIR, f"btts_model__{lg}.joblib"))
        meta = {"features": feat_cols}
        with open(os.path.join(MODEL_DIR, f"btts_model__{lg}.job.json"), "w") as f:
            json.dump(meta, f)
    return pd.DataFrame(report_rows)

def main():
    hist = safe_read(HIST, ["date","home_team","away_team","home_goals","away_goals", LEAGUE_COL])
    if hist.empty:
        pd.DataFrame(columns=["league","n","logloss","brier"]).to_csv(os.path.join(RUN_DIR,"BTTS_TRAIN_REPORT.csv"), index=False)
        print("[WARN] HIST_matches.csv missing/empty; wrote empty BTTS_TRAIN_REPORT.csv")
        return

    teamvec = build_team_vectors()
    if teamvec.empty:
        pd.DataFrame(columns=["league","n","logloss","brier"]).to_csv(os.path.join(RUN_DIR,"BTTS_TRAIN_REPORT.csv"), index=False)
        print("[WARN] No team vectors; wrote empty BTTS_TRAIN_REPORT.csv")
        return

    train_df = build_training_frame(hist, teamvec)
    if train_df.empty:
        pd.DataFrame(columns=["league","n","logloss","brier"]).to_csv(os.path.join(RUN_DIR,"BTTS_TRAIN_REPORT.csv"), index=False)
        print("[WARN] No BTTS training rows; wrote empty BTTS_TRAIN_REPORT.csv")
        return

    report = train_per_league(train_df)
    report.to_csv(os.path.join(RUN_DIR, "BTTS_TRAIN_REPORT.csv"), index=False)
    print("BTTS training complete:", len(report), "leagues")

if __name__ == "__main__":
    main()