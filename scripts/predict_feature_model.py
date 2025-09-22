# scripts/predict_feature_model.py
# Produce feature-model probabilities for upcoming fixtures (no CLI args).
# Reads:
#   data/feature_model.pkl
#   data/xg_metrics_hybrid.csv
#   data/team_statsbomb_features.csv
#   data/sd_538_spi.csv
#   data/team_form_features.csv
#   data/UPCOMING_7D_enriched.csv
# Writes:
#   data/feature_proba_upcoming.csv  (date, home_team, away_team, fH, fD, fA)

import os, json, pickle, numpy as np, pandas as pd

DATA = "data"
UP   = os.path.join(DATA, "UPCOMING_7D_enriched.csv")
HYB  = os.path.join(DATA, "xg_metrics_hybrid.csv")
SBF  = os.path.join(DATA, "team_statsbomb_features.csv")
SPI  = os.path.join(DATA, "sd_538_spi.csv")
FORM = os.path.join(DATA, "team_form_features.csv")
MOD  = os.path.join(DATA, "feature_model.pkl")
OUT  = os.path.join(DATA, "feature_proba_upcoming.csv")

def safe_read(p, cols=None):
    if not os.path.exists(p): return pd.DataFrame(columns=cols or [])
    try: df = pd.read_csv(p); 
    except Exception: return pd.DataFrame(columns=cols or [])
    if cols:
        for c in cols:
            if c not in df.columns: df[c] = np.nan
    return df

def main():
    up = safe_read(UP, ["date","home_team","away_team"])
    if up.empty:
        pd.DataFrame(columns=["date","home_team","away_team","fH","fD","fA"]).to_csv(OUT, index=False)
        print("[WARN] No upcoming fixtures; wrote empty feature_proba_upcoming.csv")
        return
    # load model
    if not os.path.exists(MOD):
        pd.DataFrame(columns=["date","home_team","away_team","fH","fD","fA"]).to_csv(OUT, index=False)
        print("[WARN] feature_model.pkl missing; wrote empty feature_proba_upcoming.csv")
        return

    pack = pickle.load(open(MOD,"rb"))
    scaler, model, feat_names = pack.get("scaler"), pack.get("model"), pack.get("feat_names",[])
    if model is None or scaler is None or not feat_names:
        pd.DataFrame(columns=["date","home_team","away_team","fH","fD","fA"]).to_csv(OUT, index=False)
        print("[WARN] Empty feature model; wrote empty feature_proba_upcoming.csv")
        return

    # Build team vectors same way as training (stationary approx)
    hyb = safe_read(HYB, ["team","xg_hybrid","xga_hybrid","xgd90_hybrid"])
    sbf = safe_read(SBF, ["team","xa_sb","psxg_minus_goals_sb","setpiece_xg_sb","openplay_xg_sb"])
    spi = safe_read(SPI); spi_cols = {c.lower(): c for c in spi.columns}
    if "team" not in spi.columns:
        if "squad" in spi.columns: spi = spi.rename(columns={"squad":"team"})
        elif "team_name" in spi.columns: spi = spi.rename(columns={"team_name":"team"})
    off_col = spi_cols.get("off") or spi_cols.get("offense")
    def_col = spi_cols.get("def") or spi_cols.get("defense")
    spi_small = pd.DataFrame(columns=["team","spi_off","spi_def"])
    if "team" in spi.columns and off_col and def_col:
        spi_small = spi.groupby("team", as_index=False)[[off_col,def_col]].mean()
        spi_small = spi_small.rename(columns={off_col:"spi_off", def_col:"spi_def"})
    form = safe_read(FORM, ["team","last5_ppg","last10_ppg","last5_xgpg","last5_xgapg","last10_xgpg","last10_xgapg"])

    teamvec = pd.DataFrame({"team": pd.unique(pd.concat([
        hyb["team"], sbf["team"], spi_small["team"], form["team"]
    ], ignore_index=True).dropna())})
    teamvec = teamvec.merge(hyb, on="team", how="left")
    if not sbf.empty:
        sbf2 = sbf.groupby("team", as_index=False).agg({"xa_sb":"mean","psxg_minus_goals_sb":"mean","setpiece_xg_sb":"mean","openplay_xg_sb":"mean"})
        tot = (sbf2["setpiece_xg_sb"] + sbf2["openplay_xg_sb"]).replace(0,np.nan)
        sbf2["setpiece_share"] = (sbf2["setpiece_xg_sb"]/tot).fillna(0.0)
        teamvec = teamvec.merge(sbf2[["team","xa_sb","psxg_minus_goals_sb","setpiece_share"]], on="team", how="left")
    if not spi_small.empty:
        teamvec = teamvec.merge(spi_small, on="team", how="left")
    if not form.empty:
        teamvec = teamvec.merge(form, on="team", how="left")

    # Same feature set as training
    base_cols = [
        "xg_hybrid","xga_hybrid","xgd90_hybrid",
        "xa_sb","psxg_minus_goals_sb","setpiece_share",
        "spi_off","spi_def",
        "last5_ppg","last10_ppg","last5_xgpg","last5_xgapg",
        "last10_ppg","last10_xgpg","last10_xgapg"
    ]
    for c in base_cols:
        if c not in teamvec.columns: teamvec[c] = np.nan

    rows = []
    for r in up.itertuples(index=False):
        ht, at = r.home_team, r.away_team
        hv = teamvec[teamvec["team"]==ht].head(1)
        av = teamvec[teamvec["team"]==at].head(1)
        if hv.empty or av.empty:
            rows.append({"date":getattr(r,"date",None), "home_team":ht, "away_team":at, "fH":np.nan,"fD":np.nan,"fA":np.nan})
            continue
        diffs = (hv[base_cols].values - av[base_cols].values)[0]
        Xs = scaler.transform([diffs])
        proba = model.predict_proba(Xs)[0]  # order [home,draw,away]
        rows.append({"date":getattr(r,"date",None), "home_team":ht, "away_team":at,
                     "fH": float(proba[0]), "fD": float(proba[1]), "fA": float(proba[2])})

    pd.DataFrame(rows).to_csv(OUT, index=False)
    print(f"[OK] wrote {OUT} rows={len(rows)}")

if __name__ == "__main__":
    main()