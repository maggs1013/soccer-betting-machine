#!/usr/bin/env python3
"""
predict_feature_model.py
Compute the SAME features used in training (windows + contrasts), then
apply saved scaler+model to produce (fH, fD, fA) for upcoming fixtures.

Reads:
  data/feature_model.pkl                (contains scaler, model, feat_names)
  data/feature_model_features.json      (feat_names)
  data/UPCOMING_7D_enriched.csv         (date, league, fixture_id, home_team, away_team)
  data/xg_metrics_hybrid.csv
  data/team_statsbomb_features.csv
  data/sd_538_spi.csv
  data/team_form_features.csv
  data/HIST_matches.csv
Writes:
  data/feature_proba_upcoming.csv       (fixture_id, date, home_team, away_team, fH, fD, fA)
"""

import os, json, pickle
import numpy as np
import pandas as pd

DATA = "data"
UP   = os.path.join(DATA, "UPCOMING_7D_enriched.csv")
HYB  = os.path.join(DATA, "xg_metrics_hybrid.csv")
SBF  = os.path.join(DATA, "team_statsbomb_features.csv")
SPI  = os.path.join(DATA, "sd_538_spi.csv")
FORM = os.path.join(DATA, "team_form_features.csv")
HIST = os.path.join(DATA, "HIST_matches.csv")

INM  = os.path.join(DATA, "feature_model.pkl")
INF  = os.path.join(DATA, "feature_model_features.json")
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
            if c not in df.columns: df[c] = np.nan
    return df

def to_season(dt):
    return pd.to_datetime(dt, errors="coerce").dt.year

def build_team_windows_from_hist(H):
    H = H.copy()
    H["date"] = pd.to_datetime(H["date"], errors="coerce")
    H = H.dropna(subset=["date"]).sort_values("date")
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
        for n in (3,5,7,10):
            row[f"last{n}_ppg"] = g["pts"].rolling(n, min_periods=1).mean().iloc[-1]
        season = g["season"].iloc[-1]
        g_season = g[g["season"]==season]
        row["season_ppg"] = g_season["pts"].mean() if not g_season.empty else np.nan
        row["goal_volatility_5"]  = g["gf"].rolling(5,  min_periods=2).var().iloc[-1]
        row["goal_volatility_10"] = g["gf"].rolling(10, min_periods=2).var().iloc[-1]
        rows.append(row)
    return pd.DataFrame(rows)

def canonical_fixture_id(row):
    date = str(row.get("date","NA")).replace("-","")
    h = str(row.get("home_team","NA")).strip().lower().replace(" ","_")
    a = str(row.get("away_team","NA")).strip().lower().replace(" ","_")
    return f"{date}__{h}__vs__{a}"

def main():
    # load model + feat list
    if not os.path.exists(INM) or not os.path.exists(INF):
        pd.DataFrame(columns=["fixture_id","date","home_team","away_team","fH","fD","fA"]).to_csv(OUT, index=False)
        print("[WARN] feature_model files missing; wrote header-only.")
        return
    model_pack = pickle.load(open(INM,"rb"))
    feat_json  = json.load(open(INF,"r"))
    scaler = model_pack.get("scaler"); clf = model_pack.get("model")
    feat_names = feat_json.get("feat_names", [])
    if scaler is None or clf is None or not feat_names:
        pd.DataFrame(columns=["fixture_id","date","home_team","away_team","fH","fD","fA"]).to_csv(OUT, index=False)
        print("[WARN] feature model empty; wrote header-only.")
        return

    up = safe_read(UP)
    if up.empty:
        pd.DataFrame(columns=["fixture_id","date","home_team","away_team","fH","fD","fA"]).to_csv(OUT, index=False)
        print("[WARN] UPCOMING empty; wrote header-only.")
        return
    if "fixture_id" not in up.columns:
        up["fixture_id"] = up.apply(canonical_fixture_id, axis=1)

    # Build team vectors
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

    tv = pd.DataFrame({"team": pd.unique(pd.concat([hyb["team"], sbf["team"], spi_small["team"]], ignore_index=True).dropna())})
    tv = tv.merge(hyb, on="team", how="left")
    if not sbf.empty:
        sbf2 = sbf.groupby("team", as_index=False).agg({"xa_sb":"mean","psxg_minus_goals_sb":"mean","setpiece_xg_sb":"mean","openplay_xg_sb":"mean"})
        tot = (sbf2["setpiece_xg_sb"] + sbf2["openplay_xg_sb"]).replace(0, np.nan)
        sbf2["setpiece_share"] = (sbf2["setpiece_xg_sb"]/tot).fillna(0.0)
        tv = tv.merge(sbf2[["team","xa_sb","psxg_minus_goals_sb","setpiece_share"]], on="team", how="left")
    if not spi_small.empty:
        tv = tv.merge(spi_small, on="team", how="left")

    # Windows from HIST
    H = safe_read(HIST, ["date","home_team","away_team","home_goals","away_goals","league"])
    win = build_team_windows_from_hist(H) if not H.empty else pd.DataFrame(columns=["team"])
    tv = tv.merge(win, on="team", how="left")

    # Minimal xG window presence via FORM last5_xgpg (if available)
    form = safe_read(FORM, ["team","last5_xgpg"])
    if not form.empty:
        tv = tv.merge(form, on="team", how="left")
    else:
        tv["last5_xgpg"] = np.nan

    # Contrasts
    tv["ppg_momentum_3_10"]     = tv["last3_ppg"]  - tv["last10_ppg"]
    tv["ppg_momentum_5_season"]  = tv["last5_ppg"]  - tv["season_ppg"]
    tv["xg_momentum_3_10"]       = tv["last3_xgpg"] - tv["last10_xgpg"]
    tv["xg_momentum_5_season"]   = tv["last5_xgpg"] - tv.get("season_xgpg", np.nan)

    # Build rows for upcoming: diffs for feat_names order
    rows = []
    meta = []
    for r in up.itertuples(index=False):
        ht, at = getattr(r, "home_team", None), getattr(r, "away_team", None)
        hv = tv[tv["team"]==ht].head(1)
        av = tv[tv["team"]==at].head(1)
        if hv.empty or av.empty:
            diffs = [np.nan]*len(feat_names)
        else:
            # strip the 'diff_' prefix to get column names
            cols = [c.replace("diff_","") for c in feat_names]
            # ensure every needed column exists
            for c in cols:
                if c not in hv.columns:
                    hv[c] = np.nan
                if c not in av.columns:
                    av[c] = np.nan
            diffs = (hv[cols].values - av[cols].values)[0]
        rows.append(diffs)
        meta.append({
            "fixture_id": getattr(r, "fixture_id", None),
            "date": getattr(r, "date", None),
            "home_team": ht,
            "away_team": at
        })

    X = np.asarray(rows, dtype=float)
    # impute like training did (median); we rely on scaler mean/var but simple fill is ok pre-transform
    col_medians = np.nanmedian(X, axis=0)
    col_medians = np.where(np.isfinite(col_medians), col_medians, 0.0)
    nr, nc = np.where(np.isnan(X))
    if nr.size: X[nr, nc] = col_medians[nc]

    Xs = scaler.transform(X)
    proba = clf.predict_proba(Xs)
    # map class order to H/D/A
    classes = list(getattr(clf, "classes_", [0,1,2]))
    idxH = classes.index(0) if 0 in classes else 0
    idxD = classes.index(1) if 1 in classes else 1
    idxA = classes.index(2) if 2 in classes else 2

    out = pd.DataFrame({
        "fixture_id": [m["fixture_id"] for m in meta],
        "date":       [m["date"] for m in meta],
        "home_team":  [m["home_team"] for m in meta],
        "away_team":  [m["away_team"] for m in meta],
        "fH": proba[:, idxH],
        "fD": proba[:, idxD],
        "fA": proba[:, idxA],
    })
    out.to_csv(OUT, index=False)
    print(f"[OK] Wrote {OUT} rows={len(out)}")

if __name__ == "__main__":
    main()