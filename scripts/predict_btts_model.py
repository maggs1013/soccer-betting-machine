#!/usr/bin/env python3
import os, json
import pandas as pd
import numpy as np
from datetime import datetime
import joblib
from sklearn.isotonic import IsotonicRegression

DATA_DIR = "data"
RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
os.makedirs(RUN_DIR, exist_ok=True)

INPUT_ENRICHED = os.path.join(DATA_DIR, "enriched_fixtures.csv")
ODDS_CSV = os.path.join(DATA_DIR, "odds_upcoming.csv")  # unified odds from your consolidate_odds
LEAGUE_COL = "league"
ID_COL = "fixture_id"
FEATURES_KEY = "features"

MODEL_DIR = os.path.join(DATA_DIR, "models_btts")
CAL_DIR = os.path.join(DATA_DIR, "calibration_btts")
os.makedirs(CAL_DIR, exist_ok=True)

# market prior field names in ODDS file
ODDS_BTTS_Y = "odds_btts_yes"
ODDS_BTTS_N = "odds_btts_no"

def implied_prob(o):
    out = np.where(o>0, 1.0/o, np.nan)
    return out

def vig_strip(p):
    if not np.isfinite(p).all():
        return p
    s = np.nansum(p, axis=1, keepdims=True)
    return np.divide(p, s, out=np.full_like(p, np.nan), where=s>0)

def load_model(league):
    path = os.path.join(MODEL_DIR, f"btts_model__{league}.joblib")
    if os.path.exists(path):
        return joblib.load(path)
    return None

def per_league_isotonic_fit(df, league):
    # Fit isotonic on recent history in that league (where label exists)
    sub = df[(df[LEAGUE_COL]==league) & df["is_train"].eq(1) & df["btts_y"].isin([0,1])]
    if len(sub)<200: return None
    iso = IsotonicRegression(out_of_bounds="clip")
    iso.fit(sub["p_model"].values, sub["btts_y"].values)
    return iso

def main():
    fixtures = pd.read_csv(INPUT_ENRICHED)
    odds = pd.read_csv(ODDS_CSV)
    df = fixtures.merge(odds[[ID_COL, ODDS_BTTS_Y, ODDS_BTTS_N]], on=ID_COL, how="left")

    pred_rows = []
    for lg, g in df.groupby(LEAGUE_COL):
        model = load_model(lg)
        if model is None:
            continue
        feats = joblib.load(os.path.join(MODEL_DIR, f"btts_model__{lg}.joblib"))
        # model pipeline has feature order embedded; pull features json for robust selection
        meta_json = os.path.join(MODEL_DIR, f"btts_model__{lg}.job.json")
        features = joblib.load(os.path.join(MODEL_DIR, f"btts_model__{lg}.joblib")).named_steps["ss"].get_feature_names_out() \
                   if hasattr(feats.named_steps["ss"], "get_feature_names_out") else None
        try:
            with open(meta_json) as f:
                meta = json.load(f)
                feat_list = meta.get("features")
        except Exception:
            feat_list = None

        use_cols = feat_list if feat_list else [c for c in g.columns if c not in (ID_COL, LEAGUE_COL)]
        X = g[use_cols].fillna(0).values
        p_model = model.predict_proba(X)[:,1]
        gg = g.copy()
        gg["p_model"] = p_model

        # market prior
        market = gg[[ODDS_BTTS_Y, ODDS_BTTS_N]].copy()
        m = np.column_stack([implied_prob(market[ODDS_BTTS_Y].values),
                             implied_prob(market[ODDS_BTTS_N].values)])
        m = vig_strip(m)
        p_yes_market = m[:,0] if m.shape[1]>=1 else np.nan

        # simple blend weight (can be replaced by per-league learned w_market)
        w_market = 0.5
        gg["p_blend_raw"] = w_market * p_yes_market + (1-w_market) * gg["p_model"]

        # isotonic calibration per league
        iso = per_league_isotonic_fit(fixtures, lg)
        if iso is not None:
            gg["p_btts_yes"] = iso.transform(gg["p_blend_raw"])
        else:
            gg["p_btts_yes"] = gg["p_blend_raw"]

        pred_rows.append(gg[[ID_COL, LEAGUE_COL, "p_model", "p_btts_yes"]])

    out = pd.concat(pred_rows, ignore_index=True) if pred_rows else pd.DataFrame(columns=[ID_COL, LEAGUE_COL, "p_model", "p_btts_yes"])
    out.to_csv(os.path.join(RUN_DIR, "PREDICTIONS_BTTS_7D.csv"), index=False)
    print("BTTS predictions written.")

if __name__ == "__main__":
    main()