#!/usr/bin/env python3
# Train a multinomial logistic feature model from existing team features (no CLI args).
# Reads:
#   data/HIST_matches.csv
#   data/xg_metrics_hybrid.csv
#   data/team_statsbomb_features.csv
#   data/sd_538_spi.csv
#   data/team_form_features.csv
# Writes:
#   data/feature_model.pkl
#   data/feature_model_features.json

import os, json, pickle
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

DATA = "data"
HIST = os.path.join(DATA, "HIST_matches.csv")
HYB  = os.path.join(DATA, "xg_metrics_hybrid.csv")
SBF  = os.path.join(DATA, "team_statsbomb_features.csv")
SPI  = os.path.join(DATA, "sd_538_spi.csv")
FORM = os.path.join(DATA, "team_form_features.csv")
OUTM = os.path.join(DATA, "feature_model.pkl")
OUTF = os.path.join(DATA, "feature_model_features.json")

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

def write_empty_model(feat_names, msg):
    pickle.dump({"scaler": None, "model": None, "feat_names": feat_names}, open(OUTM, "wb"))
    json.dump({"feat_names": feat_names}, open(OUTF, "w"))
    print(f"[WARN] {msg} → wrote empty feature model.")

def main():
    hist = safe_read(HIST, ["date","home_team","away_team","home_goals","away_goals"])
    if hist.empty:
        write_empty_model([], "HIST empty")
        return

    # Use current team-level features (stationary approximation)
    hyb = safe_read(HYB, ["team","xg_hybrid","xga_hybrid","xgd90_hybrid"])
    sbf = safe_read(SBF, ["team","xa_sb","psxg_minus_goals_sb","setpiece_xg_sb","openplay_xg_sb"])

    spi = safe_read(SPI)
    spi_cols = {c.lower(): c for c in spi.columns}
    if "team" not in spi.columns:
        if "squad" in spi.columns:      spi = spi.rename(columns={"squad":"team"})
        elif "team_name" in spi.columns: spi = spi.rename(columns={"team_name":"team"})
    off_col = spi_cols.get("off") or spi_cols.get("offense")
    def_col = spi_cols.get("def") or spi_cols.get("defense")
    spi_small = pd.DataFrame(columns=["team","spi_off","spi_def"])
    if "team" in spi.columns and off_col and def_col:
        spi_small = spi.groupby("team", as_index=False)[[off_col,def_col]].mean()
        spi_small = spi_small.rename(columns={off_col:"spi_off", def_col:"spi_def"})

    form = safe_read(FORM, ["team","last5_ppg","last10_ppg","last5_xgpg","last5_xgapg","last10_xgpg","last10_xgapg"])

    # Merge into one team vector table
    teamvec = pd.DataFrame({"team": pd.unique(pd.concat([
        hyb["team"], sbf["team"], spi_small["team"], form["team"]
    ], ignore_index=True).dropna())})
    if teamvec.empty:
        write_empty_model([], "No team vectors available from sources")
        return

    teamvec = teamvec.merge(hyb, on="team", how="left")

    # Compute set-piece share
    if not sbf.empty:
        sbf2 = sbf.groupby("team", as_index=False).agg({
            "xa_sb":"mean","psxg_minus_goals_sb":"mean",
            "setpiece_xg_sb":"mean","openplay_xg_sb":"mean"
        })
        tot = (sbf2["setpiece_xg_sb"] + sbf2["openplay_xg_sb"]).replace(0, np.nan)
        sbf2["setpiece_share"] = (sbf2["setpiece_xg_sb"]/tot).fillna(0.0)
        teamvec = teamvec.merge(sbf2[["team","xa_sb","psxg_minus_goals_sb","setpiece_share"]], on="team", how="left")

    if not spi_small.empty:
        teamvec = teamvec.merge(spi_small, on="team", how="left")
    if not form.empty:
        teamvec = teamvec.merge(form, on="team", how="left")

    # Feature columns to use (home_minus_away diffs later)
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

    # Labels: 0=home win, 1=draw, 2=away win
    y = np.where(hist["home_goals"]>hist["away_goals"],0,
         np.where(hist["home_goals"]==hist["away_goals"],1,2))

    # Build training matrix: differences (home - away)
    samples, labels = [], []
    for r, lab in zip(hist.itertuples(index=False), y):
        ht, at = r.home_team, r.away_team
        hv = teamvec[teamvec["team"]==ht].head(1)
        av = teamvec[teamvec["team"]==at].head(1)
        if hv.empty or av.empty:
            continue  # no team vector → skip
        diffs = (hv[numeric_cols].values - av[numeric_cols].values)[0]
        samples.append(diffs); labels.append(lab)

    if not samples:
        write_empty_model([f"diff_{c}" for c in numeric_cols], "No trainable pairs after merge")
        return

    X = np.asarray(samples, dtype=float)
    y2 = np.asarray(labels, dtype=int)

    # Drop rows where ALL features are NaN
    row_ok = ~np.isnan(X).all(axis=1)
    X = X[row_ok]; y2 = y2[row_ok]

    # Need enough data + all three classes for multinomial
    if X.size == 0 or np.unique(y2).size < 3:
        write_empty_model([f"diff_{c}" for c in numeric_cols], "Not enough clean samples/classes")
        return

    # Column-median imputation for remaining NaNs; if a column is entirely NaN, use 0.0
    col_medians = np.nanmedian(X, axis=0)
    col_medians = np.where(np.isfinite(col_medians), col_medians, 0.0)
    nr, nc = np.where(np.isnan(X))
    if nr.size:
        X[nr, nc] = col_medians[nc]

    # Scale and fit
    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)

    clf = LogisticRegression(multi_class="multinomial", max_iter=200, solver="lbfgs")
    clf.fit(Xs, y2)

    # Persist model with same structure
    pickle.dump({"scaler": scaler, "model": clf, "feat_names": [f"diff_{c}" for c in numeric_cols]}, open(OUTM, "wb"))
    json.dump({"feat_names": [f"diff_{c}" for c in numeric_cols]}, open(OUTF, "w"))

    print(f"[OK] trained feature model with {Xs.shape[0]} samples, {Xs.shape[1]} features → {OUTM}")

if __name__ == "__main__":
    main()