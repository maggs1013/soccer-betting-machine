#!/usr/bin/env python3
"""
predict_btts_model.py
Score upcoming fixtures with per-league BTTS models.

Reads (data/):
  UPCOMING_7D_enriched.csv
  xg_metrics_hybrid.csv
  team_statsbomb_features.csv
  sd_538_spi.csv
  team_form_features.csv
  models_btts/btts_model__{league}.joblib
  models_btts/btts_model__{league}.job.json

Writes:
  runs/YYYY-MM-DD/PREDICTIONS_BTTS_7D.csv   (fixture_id, league, p_model, p_btts_yes)
"""

import os, json
import numpy as np
import pandas as pd
import joblib
from datetime import datetime

DATA = "data"
RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
os.makedirs(RUN_DIR, exist_ok=True)

UP   = os.path.join(DATA, "UPCOMING_7D_enriched.csv")
HYB  = os.path.join(DATA, "xg_metrics_hybrid.csv")
SBF  = os.path.join(DATA, "team_statsbomb_features.csv")
SPI  = os.path.join(DATA, "sd_538_spi.csv")
FORM = os.path.join(DATA, "team_form_features.csv")
MODEL_DIR = os.path.join(DATA, "models_btts")

LEAGUE_COL = "league"
BASE_FEATS = [
    "xg_hybrid","xga_hybrid","xgd90_hybrid",
    "xa_sb","psxg_minus_goals_sb","setpiece_share",
    "spi_off","spi_def",
    "last5_ppg","last5_xgpg","last5_xgapg",
    "last10_ppg","last10_xgpg","last10_xgapg",
]

def safe_read(p, cols=None):
    if not os.path.exists(p):
        return pd.DataFrame(columns=cols or [])
    try:
        df = pd.read_csv(p)
    except Exception:
        return pd.DataFrame(columns=cols or [])
    if cols:
        for c in cols:
            if c not in df.columns:
                df[c] = np.nan
    return df

def canonical_fixture_id(row):
    date = str(row.get("date","NA")).replace("-","")
    h = str(row.get("home_team","NA")).strip().lower().replace(" ","_")
    a = str(row.get("away_team","NA")).strip().lower().replace(" ","_")
    return f"{date}__{h}__vs__{a}"

def build_team_vectors():
    hyb = safe_read(HYB, ["team","xg_hybrid","xga_hybrid","xgd90_hybrid"])
    sbf = safe_read(SBF, ["team","xa_sb","psxg_minus_goals_sb","setpiece_xg_sb","openplay_xg_sb"])
    spi = safe_read(SPI)
    form = safe_read(FORM, ["team","last5_ppg","last5_xgpg","last5_xgapg","last10_ppg","last10_xgpg","last10_xgapg"])

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

    teamvec = pd.DataFrame({
        "team": pd.unique(pd.concat([hyb["team"], sbf["team"], spi_small["team"], form["team"]], ignore_index=True).dropna())
    })
    if teamvec.empty: return teamvec

    teamvec = teamvec.merge(hyb, on="team", how="left")
    if not sbf.empty:
        sbf2 = sbf.groupby("team", as_index=False).agg({
            "xa_sb":"mean","psxg_minus_goals_sb":"mean","setpiece_xg_sb":"mean","openplay_xg_sb":"mean"
        })
        tot = (sbf2["setpiece_xg_sb"] + sbf2["openplay_xg_sb"]).replace(0,np.nan)
        sbf2["setpiece_share"] = (sbf2["setpiece_xg_sb"]/tot).fillna(0.0)
        teamvec = teamvec.merge(sbf2[["team","xa_sb","psxg_minus_goals_sb","setpiece_share"]], on="team", how="left")
    if not spi_small.empty:
        teamvec = teamvec.merge(spi_small, on="team", how="left")
    if not form.empty:
        teamvec = teamvec.merge(form, on="team", how="left")

    for c in BASE_FEATS:
        if c not in teamvec.columns:
            teamvec[c] = np.nan
    return teamvec

def main():
    up = safe_read(UP)
    if up.empty:
        pd.DataFrame(columns=["fixture_id","league","p_model","p_btts_yes"]).to_csv(os.path.join(RUN_DIR,"PREDICTIONS_BTTS_7D.csv"), index=False)
        print("[WARN] UPCOMING_7D_enriched.csv missing/empty; wrote empty PREDICTIONS_BTTS_7D.csv")
        return
    if LEAGUE_COL not in up.columns:
        up[LEAGUE_COL] = "GLOBAL"
    if "fixture_id" not in up.columns:
        up["fixture_id"] = up.apply(canonical_fixture_id, axis=1)

    tv = build_team_vectors()
    if tv.empty:
        pd.DataFrame(columns=["fixture_id","league","p_model","p_btts_yes"]).to_csv(os.path.join(RUN_DIR,"PREDICTIONS_BTTS_7D.csv"), index=False)
        print("[WARN] No team vectors; wrote empty PREDICTIONS_BTTS_7D.csv")
        return

    # Build feature rows per fixture
    feat_cols = [f"home_{c}" for c in BASE_FEATS] + [f"away_{c}" for c in BASE_FEATS]
    rows = []
    for r in up.itertuples(index=False):
        ht, at, lg = getattr(r, "home_team", None), getattr(r, "away_team", None), getattr(r, LEAGUE_COL)
        fid = getattr(r, "fixture_id", None)
        hv = tv[tv["team"]==ht].head(1)
        av = tv[tv["team"]==at].head(1)
        if hv.empty or av.empty:
            feat = {col: np.nan for col in feat_cols}
        else:
            feat = {}
            for c in BASE_FEATS:
                feat[f"home_{c}"] = hv[c].iloc[0]
                feat[f"away_{c}"] = av[c].iloc[0]
        feat["fixture_id"] = fid
        feat["league"] = lg
        rows.append(feat)
    Xdf = pd.DataFrame(rows)

    # Predict per league
    preds = []
    for lg, g in Xdf.groupby("league"):
        mdl_path = os.path.join(MODEL_DIR, f"btts_model__{lg}.joblib")
        meta_path = os.path.join(MODEL_DIR, f"btts_model__{lg}.job.json")
        if not os.path.exists(mdl_path):
            continue
        pipe = joblib.load(mdl_path)
        with open(meta_path, "r") as f:
            meta = json.load(f)
        feats = meta["features"]
        X = g[feats].values
        # pipe has imputer+scaler+lr, so we can just predict
        p = pipe.predict_proba(X)[:,1]
        tmp = pd.DataFrame({
            "fixture_id": g["fixture_id"].values,
            "league": g["league"].values,
            "p_model": p,
            "p_btts_yes": p  # (optionally blend with market later)
        })
        preds.append(tmp)

    out = pd.concat(preds, ignore_index=True) if preds else pd.DataFrame(columns=["fixture_id","league","p_model","p_btts_yes"])
    out.to_csv(os.path.join(RUN_DIR, "PREDICTIONS_BTTS_7D.csv"), index=False)
    print("[OK] PREDICTIONS_BTTS_7D.csv written:", len(out))

if __name__ == "__main__":
    main()