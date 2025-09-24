#!/usr/bin/env python3
# Train a multinomial logistic feature model using domestic-only results for windows & samples,
# while keeping robustness (missing league → GLOBAL) and time-decay weighting.
# Safe momentum computations (no KeyErrors) via Series.get(..., NaN-series) fallbacks.

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

HALF_LIFE_DAYS = 180.0

UEFA_LEAGUE_TOKENS = [
    "champions league", "uefa champions", "ucl",
    "europa league", "uel",
    "conference league", "uefa europa conference",
    "super cup"
]

def is_uefa(s):
    if not isinstance(s,str): return False
    s2 = s.lower()
    return any(tok in s2 for tok in UEFA_LEAGUE_TOKENS)

def safe_read(p, cols=None):
    if not os.path.exists(p):
        return pd.DataFrame(columns=cols or [])
    try:
        df = pd.read_csv(p)
    except Exception:
        return pd.DataFrame(columns=cols or [])
    if cols:
        for c in cols:
            if c not in df.columns: df[c] = np.nan
    return df

def write_empty_model(feat_names, msg):
    pickle.dump({"scaler": None, "model": None, "feat_names": feat_names}, open(OUTM, "wb"))
    json.dump({"feat_names": feat_names}, open(OUTF, "w"))
    print(f"[WARN] {msg} → wrote empty feature model.")

def build_long(H):
    H = H.copy()
    H["date"] = pd.to_datetime(H["date"], errors="coerce")
    H = H.dropna(subset=["date"]).sort_values("date")
    if "league" not in H.columns: H["league"] = "GLOBAL"
    H["is_uefa"] = H["league"].astype(str).apply(is_uefa).astype(int)
    h = H[["date","home_team","home_goals","away_goals","league","is_uefa"]].rename(
        columns={"home_team":"team","home_goals":"gf","away_goals":"ga"})
    a = H[["date","away_team","home_goals","away_goals","league","is_uefa"]].rename(
        columns={"away_team":"team","away_goals":"gf","home_goals":"ga"})
    long = pd.concat([h,a], ignore_index=True).sort_values("date")
    long["pts"] = 0
    long.loc[long["gf"] > long["ga"], "pts"] = 3
    long.loc[long["gf"] == long["ga"], "pts"] = 1
    return long

def windows_ppg(long_dom, team, n):
    g = long_dom[long_dom["team"]==team]
    if g.empty: return np.nan
    return g["pts"].rolling(n, min_periods=1).mean().iloc[-1]

def season_ppg(long_dom, team, year):
    g = long_dom[(long_dom["team"]==team) & (long_dom["date"].dt.year==year)]
    if g.empty: return np.nan
    return g["pts"].mean()

def main():
    hist = safe_read(HIST, ["date","home_team","away_team","home_goals","away_goals","league"])
    if hist.empty:
        write_empty_model([], "HIST empty"); return

    long_all = build_long(hist)
    long_dom = long_all[long_all["is_uefa"]==0]  # domestic only

    # Build team list from available sources
    hyb = safe_read(HYB, ["team","xg_hybrid","xga_hybrid","xgd90_hybrid"])
    sbf = safe_read(SBF, ["team","xa_sb","psxg_minus_goals_sb","setpiece_xg_sb","openplay_xg_sb"])
    spi = safe_read(SPI)
    spi_cols = {c.lower(): c for c in spi.columns}
    if "team" not in spi.columns:
        if "squad" in spi.columns: spi = spi.rename(columns={"squad":"team"})
        elif "team_name" in spi.columns: spi = spi.rename(columns={"team_name":"team"})
    off_col = spi_cols.get("off") or spi_cols.get("offense")
    def_col = spi_cols.get("def") or spi_cols.get("defense")
    spi_small = pd.DataFrame(columns=["team","spi_off","spi_def"])
    if "team" in spi.columns and off_col and def_col:
        spi_small = spi.groupby("team", as_index=False)[[off_col,def_col]].mean()
        spi_small = spi_small.rename(columns={off_col:"spi_off", def_col:"spi_def"})

    teamvec = pd.DataFrame({"team": pd.unique(pd.concat([hyb["team"], sbf["team"], spi_small["team"]], ignore_index=True).dropna())})
    if teamvec.empty:
        write_empty_model([], "No team vectors available")
        return

    # Merge sources
    teamvec = teamvec.merge(hyb, on="team", how="left")
    if not sbf.empty:
        sbf2 = sbf.groupby("team", as_index=False).agg({
            "xa_sb":"mean","psxg_minus_goals_sb":"mean","setpiece_xg_sb":"mean","openplay_xg_sb":"mean"
        })
        tot = (sbf2["setpiece_xg_sb"] + sbf2["openplay_xg_sb"]).replace(0, np.nan)
        sbf2["setpiece_share"] = (sbf2["setpiece_xg_sb"]/tot).fillna(0.0)
        teamvec = teamvec.merge(sbf2[["team","xa_sb","psxg_minus_goals_sb","setpiece_share"]], on="team", how="left")
    if not spi_small.empty:
        teamvec = teamvec.merge(spi_small, on="team", how="left")

    # Compute domestic-only windows for each team (anchor to most recent date in hist)
    last_date = long_dom["date"].max() if not long_dom.empty else pd.Timestamp.utcnow()
    win_rows = []
    for team in teamvec["team"].dropna().unique():
        row = {"team": team}
        row["last3_ppg"]  = windows_ppg(long_dom, team, 3)
        row["last5_ppg"]  = windows_ppg(long_dom, team, 5)
        row["last7_ppg"]  = windows_ppg(long_dom, team, 7)
        row["last10_ppg"] = windows_ppg(long_dom, team, 10)
        row["season_ppg"] = season_ppg(long_dom, team, last_date.year)
        # volatility (domestic)
        g = long_dom[long_dom["team"]==team]
        if g.empty:
            row["goal_volatility_5"] = np.nan
            row["goal_volatility_10"] = np.nan
        else:
            row["goal_volatility_5"]  = g["gf"].rolling(5,  min_periods=2).var().iloc[-1]
            row["goal_volatility_10"] = g["gf"].rolling(10, min_periods=2).var().iloc[-1]
        win_rows.append(row)
    tv_win = pd.DataFrame(win_rows)

    teamvec = teamvec.merge(tv_win, on="team", how="left")

    # Ensure required window columns exist (create NaN columns if missing)
    ensure_cols = [
        "last3_ppg","last5_ppg","last7_ppg","last10_ppg","season_ppg",
        "goal_volatility_5","goal_volatility_10"
    ]
    for c in ensure_cols:
        if c not in teamvec.columns:
            teamvec[c] = np.nan

    # Minimal xG window presence: last5_xgpg from FORM (proxy)
    form = safe_read(FORM, ["team","last5_xgpg"])
    if not form.empty:
        teamvec = teamvec.merge(form, on="team", how="left")
    else:
        teamvec["last5_xgpg"] = np.nan

    # ---- SAFE momentum computations (no KeyError) ----
    nan_series = pd.Series(np.nan, index=teamvec.index)
    last3_ppg   = teamvec.get("last3_ppg",   nan_series)
    last10_ppg  = teamvec.get("last10_ppg",  nan_series)
    last5_ppg   = teamvec.get("last5_ppg",   nan_series)
    season_ppg  = teamvec.get("season_ppg",  nan_series)

    teamvec["ppg_momentum_3_10"]     = last3_ppg - last10_ppg
    teamvec["ppg_momentum_5_season"] = last5_ppg - season_ppg

    # Feature list
    numeric_cols = [
        "xg_hybrid","xga_hybrid","xgd90_hybrid",
        "xa_sb","psxg_minus_goals_sb","setpiece_share",
        "spi_off","spi_def",
        "last3_ppg","last5_ppg","last10_ppg","season_ppg",
        "ppg_momentum_3_10","ppg_momentum_5_season",
        "goal_volatility_5","goal_volatility_10",
        "last5_xgpg"
    ]
    for c in numeric_cols:
        if c not in teamvec.columns:
            teamvec[c] = np.nan

    # Build domestic-only training samples
    dom_matches = hist.copy()
    if "league" not in dom_matches.columns: dom_matches["league"]="GLOBAL"
    dom_matches = dom_matches[~dom_matches["league"].astype(str).apply(is_uefa)]
    dom_matches["date"] = pd.to_datetime(dom_matches["date"], errors="coerce")
    dom_matches = dom_matches.dropna(subset=["date"]).sort_values("date")

    y = np.where(dom_matches["home_goals"]>dom_matches["away_goals"],0,
         np.where(dom_matches["home_goals"]==dom_matches["away_goals"],1,2))

    samples, labels, dates = [], [], []
    for r, lab in zip(dom_matches.itertuples(index=False), y):
        hv = teamvec[teamvec["team"]==r.home_team].head(1)
        av = teamvec[teamvec["team"]==r.away_team].head(1)
        if hv.empty or av.empty: continue
        diffs = (hv[numeric_cols].values - av[numeric_cols].values)[0]
        samples.append(diffs); labels.append(lab); dates.append(r.date)

    if not samples:
        write_empty_model([f"diff_{c}" for c in numeric_cols], "No domestic trainable pairs")
        return

    X = np.asarray(samples, dtype=float)
    y2= np.asarray(labels, dtype=int)
    d_arr = pd.to_datetime(pd.Series(dates), errors="coerce")
    max_date = hist["date"].max()

    # drop fully-NaN rows
    mask = ~np.isnan(X).all(axis=1)
    X = X[mask]; y2 = y2[mask]; d_arr = d_arr[mask]
    if X.size==0 or np.unique(y2).size<3:
        write_empty_model([f"diff_{c}" for c in numeric_cols], "Not enough clean domestic samples/classes")
        return

    # Impute
    col_medians = np.nanmedian(X, axis=0)
    col_medians = np.where(np.isfinite(col_medians), col_medians, 0.0)
    nr, nc = np.where(np.isnan(X))
    if nr.size: X[nr, nc] = col_medians[nc]

    # Time-decay weights
    days_back = (max_date - d_arr).dt.days.clip(lower=0).astype(float)
    weights = np.power(0.5, days_back / HALF_LIFE_DAYS)

    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)

    clf = LogisticRegression(multi_class="multinomial", max_iter=300, solver="lbfgs")
    clf.fit(Xs, y2, sample_weight=weights.values)

    feat_names = [f"diff_{c}" for c in numeric_cols]
    pickle.dump({"scaler": scaler, "model": clf, "feat_names": feat_names}, open(OUTM, "wb"))
    json.dump({"feat_names": feat_names}, open(OUTF, "w"))
    print(f"[OK] trained feature model (domestic-only + decay) with {Xs.shape[0]} samples, {Xs.shape[1]} feats → {OUTM}")

if __name__ == "__main__":
    main()