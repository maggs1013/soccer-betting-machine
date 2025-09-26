#!/usr/bin/env python3
"""
trainer_minimal.py — rolling-origin CV + per-league isotonic, ECE/Brier, permutation importance
Inputs:
  data/TRAIN_MATRIX.csv   (must include: date, league, target, feature columns; optional: fixture_id)
Outputs:
  reports/TRAINING_REPORT.md
  data/PER_LEAGUE_CALIB.pkl    (dict league -> isotonic calibrator)
Notes:
  - Uses scikit-learn: LogisticRegression, IsotonicRegression, PermutationImportance (from sklearn.inspection)
  - If TRAIN_MATRIX.csv missing, exits safely.
"""

import os, pickle, json, numpy as np, pandas as pd
from datetime import datetime
from sklearn.linear_model import LogisticRegression
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import brier_score_loss, log_loss
from sklearn.inspection import permutation_importance

DATA="data"; REP="reports"
TRAIN=os.path.join(DATA,"TRAIN_MATRIX.csv")
OUTREP=os.path.join(REP,"TRAINING_REPORT.md")
CALIB=os.path.join(DATA,"PER_LEAGUE_CALIB.pkl")

def safe_read(p):
    if not os.path.exists(p): return pd.DataFrame()
    try: return pd.read_csv(p)
    except Exception: return pd.DataFrame()

def ece(probs, y, bins=10):
    if len(probs)==0: return np.nan
    bins_edges = np.linspace(0,1,bins+1)
    idx = np.digitize(probs, bins_edges, right=True)
    ece_val = 0.0; n=len(probs)
    for b in range(1,bins+1):
        mask = idx==b
        if mask.any():
            avg_p = probs[mask].mean()
            avg_y = y[mask].mean()
            ece_val += abs(avg_p-avg_y) * (mask.sum()/n)
    return ece_val

def main():
    os.makedirs(REP, exist_ok=True)
    df = safe_read(TRAIN)
    if df.empty or "target" not in df.columns or "league" not in df.columns or "date" not in df.columns:
        with open(OUTREP,"w",encoding="utf-8") as f:
            f.write("# TRAINING REPORT\n\n- No TRAIN_MATRIX.csv with required columns; trainer skipped.\n")
        print("trainer_minimal: no training matrix; skipped.")
        return

    # Prepare
    df["date"]=pd.to_datetime(df["date"], errors="coerce")
    df=df.dropna(subset=["date"]).sort_values("date")
    y = df["target"].values
    # Feature selection: all numeric except ids/context
    drop_cols = {"fixture_id","target","date","league","home_team","away_team"}
    Xcols = [c for c in df.columns if c not in drop_cols and str(df[c].dtype).startswith(("float","int"))]
    if not Xcols:
        with open(OUTREP,"w",encoding="utf-8") as f:
            f.write("# TRAINING REPORT\n\n- No numeric features; trainer skipped.\n")
        print("trainer_minimal: no numeric features; skipped."); return

    leagues = sorted(df["league"].dropna().unique().tolist())
    calib_map = {}
    lines = ["# TRAINING REPORT", f"- Rows: {len(df)}", f"- Features: {len(Xcols)}", f"- Leagues: {len(leagues)}", ""]

    # Rolling-origin CV (3 folds)
    dates = df["date"].sort_values().unique()
    if len(dates)<200: split_pts = [int(len(dates)*0.6), int(len(dates)*0.8)]
    else: split_pts = [int(len(dates)*0.7), int(len(dates)*0.85)]
    folds=[]
    for i,cut in enumerate(split_pts, start=1):
        train_end = dates[cut]
        valid_end = dates[min(cut+int(len(dates)*0.1), len(dates)-1)]
        folds.append((train_end, valid_end))

    # Fit per-league base → isotonic
    for lg in leagues:
        dfl = df[df["league"]==lg]
        if len(dfl)<200: continue
        lines.append(f"## League: {lg}  (rows={len(dfl)})")
        Xl = dfl[Xcols].values; yl = dfl["target"].values
        dl = dfl["date"].values

        # base model
        base = LogisticRegression(max_iter=200, n_jobs=None, solver="lbfgs")
        # simple rolling CV metrics
        eces=[]; briers=[]; logs=[]
        for (train_end, valid_end) in folds:
            tr = dfl["date"]<=train_end
            va = (dfl["date"]>train_end) & (dfl["date"]<=valid_end)
            if tr.sum()<100 or va.sum()<50: continue
            base.fit(dfl.loc[tr,Xcols], dfl.loc[tr,"target"])
            p = base.predict_proba(dfl.loc[va,Xcols])[:,1]
            yv= dfl.loc[va,"target"].values
            eces.append(ece(p, yv)); briers.append(brier_score_loss(yv, p)); 
            try:
                logs.append(log_loss(yv, p, eps=1e-15))
            except Exception:
                pass

        if eces:
            lines.append(f"- CV ECE: {np.mean(eces):.4f}  (bins=10)")
            lines.append(f"- CV Brier: {np.mean(briers):.4f}")
            if logs: lines.append(f"- CV LogLoss: {np.mean(logs):.4f}")
        else:
            lines.append("- CV insufficient data for folds; skipping metrics.")

        # fit on all, then isotonic using tail (most recent 20% for calibration)
        split = int(len(dfl)*0.8)
        base.fit(dfl.iloc[:split][Xcols], dfl.iloc[:split]["target"])
        p_all = base.predict_proba(dfl.iloc[split:][Xcols])[:,1]
        y_all = dfl.iloc[split:]["target"].values
        iso = IsotonicRegression(out_of_bounds="clip")
        try:
            iso.fit(p_all, y_all)
            calib_map[lg] = {"isotonic_x": p_all.tolist(), "isotonic_y": y_all.tolist()}  # simple artifact
        except Exception:
            calib_map[lg] = None

        # permutation importance (on tail)
        try:
            pi = permutation_importance(base, dfl.iloc[split:][Xcols], y_all, n_repeats=5, random_state=42)
            order = np.argsort(-pi.importances_mean)
            top = [(Xcols[i], float(pi.importances_mean[i])) for i in order[:10]]
            lines.append("- Top 10 permutation importance:")
            for name,val in top: lines.append(f"  - {name}: {val:.4f}")
        except Exception:
            lines.append("- Permutation importance failed (skipping).")

        lines.append("")

    # Save calibrators (for demo we save the mapping payload; your production can save actual fit objects)
    with open(CALIB, "wb") as f:
        pickle.dump(calib_map, f)

    with open(OUTREP,"w",encoding="utf-8") as f:
        f.write("\n".join(lines)+"\n")
    print(f"trainer_minimal: wrote {OUTREP} and {CALIB}")

if __name__ == "__main__":
    main()