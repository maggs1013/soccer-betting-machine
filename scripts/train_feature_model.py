#!/usr/bin/env python3
# Train a multinomial logistic feature model using multiple windows & contrasts,
# with time-decay weighting so recent matches matter more.

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

HALF_LIFE_DAYS = 180.0  # adjust to tune recency

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

def to_season(dt):
    return pd.to_datetime(dt, errors="coerce").dt.year

def build_team_windows_from_hist(H):
    """Build per-team rolling windows from HIST for PPG and goal volatility."""
    H = H.copy()
    H["date"] = pd.to_datetime(H["date"], errors="coerce")
    H = H.dropna(subset=["date"]).sort_values("date")
    # long format
    h = H[["date","home_team","home_goals","away_goals","league"]].rename(
        columns={"home_team":"team","home_goals":"gf"})
    h["ga"] = H["away_goals"]
    a = H[["date","away_team","home_goals","away_goals","league"]].rename(
        columns={"away_team":"team","away_goals":"gf"})
    a["ga"] = H["home_goals"]
    long = pd.concat([h,a], ignore_index=True).sort_values("date")
    long["season"] = to_season(long["date"])
    long["pts"] = 0
    long.loc[long["gf"]>long["ga"], "pts"] = 3
    long.loc[long["gf"]==long["ga"], "pts"] = 1

    rows = []
    for team, g in long.groupby("team"):
        g = g.sort_values("date")
        row = {"team": team}
        # PPG windows
        for n in (3,5,7,10):
            row[f"last{n}_ppg"] = g["pts"].rolling(n, min_periods=1).mean().iloc[-1]
        # season ppg
        season = g["season"].iloc[-1]
        g_season = g[g["season"]==season]
        row["season_ppg"] = g_season["pts"].mean() if not g_season.empty else np.nan
        # goal volatility
        row["goal_volatility_5"]  = g["gf"].rolling(5,  min_periods=2).var().iloc[-1]
        row["goal_volatility_10"] = g["gf"].rolling(10, min_periods=2).var().iloc[-1]
        rows.append(row)
    return pd.DataFrame(rows)

def main():
    hist = safe_read(HIST, ["date","home_team","away_team","home_goals","away_goals"])
    if hist.empty:
        write_empty_model([], "HIST empty")
        return
    hist["date"] = pd.to_datetime(hist["date"], errors="coerce")
    hist = hist.dropna(subset=["date"]).sort_values("date")
    max_date = hist["date"].max()

    # team-level sources
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

    form = safe_read(FORM, ["team","last5_xgpg"])  # optional for finishing eff alignment later

    # merge to build teamvec
    teamvec = pd.DataFrame({"team": pd.unique(pd.concat([hyb["team"], sbf["team"], spi_small["team"]], ignore_index=True).dropna())})
    if teamvec.empty:
        write_empty_model([], "No team vectors available")
        return
    teamvec = teamvec.merge(hyb, on="team", how="left")
    if not sbf.empty:
        sbf2 = sbf.groupby("team", as_index=False).agg({"xa_sb":"mean","psxg_minus_goals_sb":"mean","setpiece_xg_sb":"mean","openplay_xg_sb":"mean"})
        tot = (sbf2["setpiece_xg_sb"] + sbf2["openplay_xg_sb"]).replace(0, np.nan)
        sbf2["setpiece_share"] = (sbf2["setpiece_xg_sb"]/tot).fillna(0.0)
        teamvec = teamvec.merge(sbf2[["team","xa_sb","psxg_minus_goals_sb","setpiece_share"]], on="team", how="left")
    if not spi_small.empty:
        teamvec = teamvec.merge(spi_small, on="team", how="left")

    # derive windows from HIST (PPG windows, volatility)
    tv_win = build_team_windows_from_hist(hist)
    teamvec = teamvec.merge(tv_win, on="team", how="left")

    # xG momentum proxies (we only have last5_xgpg in FORM)
    if not form.empty:
        teamvec = teamvec.merge(form.rename(columns={"team":"team"}), on="team", how="left")
        teamvec["last5_xgpg"] = teamvec["last5_xgpg"].where(np.isfinite(teamvec["last5_xgpg"]), np.nan)
        # for lack of historical xG timeline, keep last5_xgpg; others remain NaN
    else:
        teamvec["last5_xgpg"] = np.nan

    # Add contrasts
    teamvec["ppg_momentum_3_10"]     = teamvec["last3_ppg"]  - teamvec["last10_ppg"]
    teamvec["ppg_momentum_5_season"] = teamvec["last5_ppg"]  - teamvec["season_ppg"]
    teamvec["xg_momentum_3_10"]      = teamvec["last3_xgpg"] - teamvec["last10_xgpg"]
    teamvec["xg_momentum_5_season"]  = teamvec["last5_xgpg"] - teamvec.get("season_xgpg", np.nan)

    # Assemble feature list (keep compact, high signal)
    numeric_cols = [
        # core quality
        "xg_hybrid","xga_hybrid","xgd90_hybrid",
        "xa_sb","psxg_minus_goals_sb","setpiece_share",
        "spi_off","spi_def",
        # windows
        "last3_ppg","last5_ppg","last10_ppg","season_ppg",
        # contrasts
        "ppg_momentum_3_10","ppg_momentum_5_season",
        # volatility proxy
        "goal_volatility_5","goal_volatility_10",
        # minimal xG window presence
        "last5_xgpg"
    ]
    for c in numeric_cols:
        if c not in teamvec.columns:
            teamvec[c] = np.nan

    # Labels 0/1/2
    y = np.where(hist["home_goals"]>hist["away_goals"],0,
         np.where(hist["home_goals"]==hist["away_goals"],1,2))

    # Build diff matrix
    samples, labels, dates = [], [], []
    for r, lab in zip(hist.itertuples(index=False), y):
        hv = teamvec[teamvec["team"]==r.home_team].head(1)
        av = teamvec[teamvec["team"]==r.away_team].head(1)
        if hv.empty or av.empty: continue
        diffs = (hv[numeric_cols].values - av[numeric_cols].values)[0]
        samples.append(diffs); labels.append(lab); dates.append(r.date)

    if not samples:
        write_empty_model([f"diff_{c}" for c in numeric_cols], "No trainable pairs")
        return

    X = np.asarray(samples, dtype=float)
    y2= np.asarray(labels, dtype=int)
    d_arr = pd.to_datetime(pd.Series(dates), errors="coerce")

    # drop fully-NaN rows
    mask = ~np.isnan(X).all(axis=1)
    X = X[mask]; y2 = y2[mask]; d_arr = d_arr[mask]
    if X.size==0 or np.unique(y2).size<3:
        write_empty_model([f"diff_{c}" for c in numeric_cols], "Not enough clean samples/classes")
        return

    # impute
    col_medians = np.nanmedian(X, axis=0)
    col_medians = np.where(np.isfinite(col_medians), col_medians, 0.0)
    nr, nc = np.where(np.isnan(X))
    if nr.size: X[nr, nc] = col_medians[nc]

    # time-decay sample weights
    days_back = (max_date - d_arr).dt.days.clip(lower=0).astype(float)
    weights = np.power(0.5, days_back / HALF_LIFE_DAYS)

    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)

    clf = LogisticRegression(multi_class="multinomial", max_iter=300, solver="lbfgs")
    clf.fit(Xs, y2, sample_weight=weights.values)

    feat_names = [f"diff_{c}" for c in numeric_cols]
    pickle.dump({"scaler": scaler, "model": clf, "feat_names": feat_names}, open(OUTM, "wb"))
    json.dump({"feat_names": feat_names}, open(OUTF, "w"))
    print(f"[OK] trained feature model (windows+decay) with {Xs.shape[0]} samples, {Xs.shape[1]} feats → {OUTM}")

if __name__ == "__main__":
    main()