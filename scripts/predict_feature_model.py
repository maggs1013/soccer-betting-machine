#!/usr/bin/env python3
"""
predict_feature_model.py
Predict upcoming 1X2 probabilities using the trained feature model.

Reads:
  data/feature_model.pkl                (scaler + model + feat_names)
  data/UPCOMING_7D_enriched.csv         (date, league, fixture_id, home_team, away_team)
  data/xg_metrics_hybrid.csv
  data/team_statsbomb_features.csv
  data/sd_538_spi.csv
  data/team_form_features.csv

Writes:
  data/feature_proba_upcoming.csv       (fixture_id, date, home_team, away_team, fH, fD, fA)

Notes:
  • NaN-safe: drops rows that are all-NaN, imputes column medians for remaining NaNs, 0.0 if a column is entirely NaN.
  • Uses the exact same feature construction as training (home-away diffs of numeric columns).
"""

import os
import json
import pickle
import numpy as np
import pandas as pd

DATA = "data"
UP   = os.path.join(DATA, "UPCOMING_7D_enriched.csv")
HYB  = os.path.join(DATA, "xg_metrics_hybrid.csv")
SBF  = os.path.join(DATA, "team_statsbomb_features.csv")
SPI  = os.path.join(DATA, "sd_538_spi.csv")
FORM = os.path.join(DATA, "team_form_features.csv")
INM  = os.path.join(DATA, "feature_model.pkl")
OUT  = os.path.join(DATA, "feature_proba_upcoming.csv")

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

def write_empty(msg=""):
    pd.DataFrame(columns=["fixture_id","date","home_team","away_team","fH","fD","fA"]).to_csv(OUT, index=False)
    if msg:
        print(f"[WARN] {msg} → wrote header-only {OUT}")
    else:
        print(f"[WARN] Wrote header-only {OUT}")

def canonical_fixture_id(row: pd.Series) -> str:
    date = str(row.get("date","NA")).replace("-","")
    h = str(row.get("home_team","NA")).strip().lower().replace(" ","_")
    a = str(row.get("away_team","NA")).strip().lower().replace(" ","_")
    return f"{date}__{h}__vs__{a}"

def build_team_vectors():
    """Rebuild the same team vector table used during training."""
    hyb = safe_read(HYB, ["team","xg_hybrid","xga_hybrid","xgd90_hybrid"])
    sbf = safe_read(SBF, ["team","xa_sb","psxg_minus_goals_sb","setpiece_xg_sb","openplay_xg_sb"])
    spi = safe_read(SPI)
    form = safe_read(FORM, [
        "team","last5_ppg","last10_ppg","last5_xgpg","last5_xgapg","last10_xgpg","last10_xgapg"
    ])

    # SPI: find team/off/def
    spi_cols = {c.lower(): c for c in spi.columns}
    if "team" not in spi.columns:
        if "squad" in spi.columns:
            spi = spi.rename(columns={"squad": "team"})
        elif "team_name" in spi.columns:
            spi = spi.rename(columns={"team_name": "team"})
    off_col = spi_cols.get("off") or spi_cols.get("offense")
    def_col = spi_cols.get("def") or spi_cols.get("defense")
    spi_small = pd.DataFrame(columns=["team","spi_off","spi_def"])
    if "team" in spi.columns and off_col and def_col:
        spi_small = spi.groupby("team", as_index=False)[[off_col, def_col]].mean()
        spi_small = spi_small.rename(columns={off_col: "spi_off", def_col: "spi_def"})

    # base team list
    teamvec = pd.DataFrame({
        "team": pd.unique(pd.concat([
            hyb["team"], sbf["team"], spi_small["team"], form["team"]
        ], ignore_index=True).dropna())
    })
    if teamvec.empty:
        return teamvec  # empty

    teamvec = teamvec.merge(hyb, on="team", how="left")

    # StatsBomb set-piece share
    if not sbf.empty:
        sbf2 = sbf.groupby("team", as_index=False).agg({
            "xa_sb": "mean",
            "psxg_minus_goals_sb": "mean",
            "setpiece_xg_sb": "mean",
            "openplay_xg_sb": "mean"
        })
        tot = (sbf2["setpiece_xg_sb"] + sbf2["openplay_xg_sb"]).replace(0, np.nan)
        sbf2["setpiece_share"] = (sbf2["setpiece_xg_sb"] / tot).fillna(0.0)
        teamvec = teamvec.merge(
            sbf2[["team","xa_sb","psxg_minus_goals_sb","setpiece_share"]],
            on="team", how="left"
        )

    if not spi_small.empty:
        teamvec = teamvec.merge(spi_small, on="team", how="left")

    if not form.empty:
        teamvec = teamvec.merge(form, on="team", how="left")

    # numeric feature list must match training script
    numeric_cols = [
        "xg_hybrid","xga_hybrid","xgd90_hybrid",
        "xa_sb","psxg_minus_goals_sb","setpiece_share",
        "spi_off","spi_def",
        "last5_ppg","last10_ppg","last5_xgpg","last5_xgapg",
        "last10_ppg","last10_xgpg","last10_xgapg"
    ]
    for c in numeric_cols:
        if c not in teamvec.columns:
            teamvec[c] = np.nan

    return teamvec, numeric_cols

def main():
    # load model
    if not os.path.exists(INM):
        write_empty("feature_model.pkl missing")
        return

    try:
        model_pack = pickle.load(open(INM, "rb"))
    except Exception:
        write_empty("feature_model.pkl unreadable")
        return

    scaler = model_pack.get("scaler")
    clf    = model_pack.get("model")
    feat_names = model_pack.get("feat_names", [])
    if scaler is None or clf is None or not feat_names:
        write_empty("feature model is empty")
        return

    # load upcoming fixtures
    up = safe_read(UP)
    if up.empty:
        write_empty("UPCOMING_7D_enriched.csv missing/empty")
        return
    if "fixture_id" not in up.columns:
        up["fixture_id"] = up.apply(canonical_fixture_id, axis=1)

    # build team vectors and numeric feature list
    tv_res = build_team_vectors()
    if isinstance(tv_res, pd.DataFrame) and tv_res.empty:
        write_empty("No team vectors available")
        return
    teamvec, numeric_cols = tv_res

    # compute diffs (home - away) for each upcoming fixture
    rows = []
    meta = []
    for r in up.itertuples(index=False):
        ht = getattr(r, "home_team", None)
        at = getattr(r, "away_team", None)
        hv = teamvec[teamvec["team"] == ht].head(1)
        av = teamvec[teamvec["team"] == at].head(1)
        if hv.empty or av.empty:
            diffs = np.full(len(numeric_cols), np.nan, dtype=float)
        else:
            diffs = (hv[numeric_cols].values - av[numeric_cols].values)[0]
        rows.append(diffs)
        meta.append({
            "fixture_id": getattr(r, "fixture_id", None),
            "date": getattr(r, "date", None),
            "home_team": ht,
            "away_team": at
        })

    if not rows:
        write_empty("No fixtures to score")
        return

    X_raw = np.asarray(rows, dtype=float)

    # drop rows where ALL features are NaN (cannot impute)
    row_ok = ~np.isnan(X_raw).all(axis=1)
    X = X_raw[row_ok]
    meta_ok = [m for m, keep in zip(meta, row_ok) if keep]

    if X.size == 0:
        write_empty("All upcoming diffs are fully NaN")
        return

    # median impute remaining NaNs (per column); 0.0 if column entirely NaN
    col_medians = np.nanmedian(X, axis=0)
    col_medians = np.where(np.isfinite(col_medians), col_medians, 0.0)
    nan_rows, nan_cols = np.where(np.isnan(X))
    if nan_rows.size:
        X[nan_rows, nan_cols] = col_medians[nan_cols]

    # scale and predict
    Xs = scaler.transform(X)
    proba = clf.predict_proba(Xs)  # shape (n, 3) but class order may vary

    # map columns to (home, draw, away) according to clf.classes_
    # classes should be [0,1,2] → home,draw,away as in training; guard anyway
    cols = {int(c): i for i, c in enumerate(getattr(clf, "classes_", [0,1,2]))}
    idxH = cols.get(0, 0)
    idxD = cols.get(1, 1)
    idxA = cols.get(2, 2)

    out = pd.DataFrame({
        "fixture_id": [m["fixture_id"] for m in meta_ok],
        "date":       [m["date"] for m in meta_ok],
        "home_team":  [m["home_team"] for m in meta_ok],
        "away_team":  [m["away_team"] for m in meta_ok],
        "fH": proba[:, idxH],
        "fD": proba[:, idxD],
        "fA": proba[:, idxA],
    })

    out.to_csv(OUT, index=False)
    print(f"[OK] Wrote {OUT} rows={len(out)}")

if __name__ == "__main__":
    main()