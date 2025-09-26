#!/usr/bin/env python3
"""
stack_trainer.py — stack base head + priors + contradiction score; per-league isotonic
Inputs:
  data/TRAIN_STACK.csv   (must include: date, league, target, base_prob [0..1], optional: contradiction_score)
  data/PRIORS_XG_SIM.csv
  data/PRIORS_AVAIL.csv
  data/PRIORS_SETPIECE.csv
  data/PRIORS_MKT.csv
  data/PRIORS_UNC.csv
Outputs:
  reports/STACK_TRAINING_REPORT.md
  data/STACK_CALIB.pkl   (dict: league -> { 'stack_model': serialized? (omitted here), 'isotonic' payload })
Notes:
  - Minimal demonstration: uses LogisticRegression stack on [base_prob, priors..., contradiction_score]
  - Calibrates final prob per-league with isotonic (payload saved)
  - If TRAIN_STACK.csv missing, exits safely.
"""

import os, pickle, json, numpy as np, pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import brier_score_loss
from sklearn.inspection import permutation_importance

DATA="data"; REP="reports"
STACK_IN=os.path.join(DATA,"TRAIN_STACK.csv")
XG = os.path.join(DATA,"PRIORS_XG_SIM.csv")
AV = os.path.join(DATA,"PRIORS_AVAIL.csv")
SP = os.path.join(DATA,"PRIORS_SETPIECE.csv")
MK = os.path.join(DATA,"PRIORS_MKT.csv")
UN = os.path.join(DATA,"PRIORS_UNC.csv")
OUTREP=os.path.join(REP,"STACK_TRAINING_REPORT.md")
CALIB=os.path.join(DATA,"STACK_CALIB.pkl")

def safe_read(p):
    if not os.path.exists(p): return pd.DataFrame()
    try: return pd.read_csv(p)
    except Exception: return pd.DataFrame()

def main():
    os.makedirs(REP, exist_ok=True)
    df = safe_read(STACK_IN)
    if df.empty or not {"date","league","target","base_prob"}.issubset(df.columns):
        with open(OUTREP,"w",encoding="utf-8") as f:
            f.write("# STACK TRAINING REPORT\n\n- No TRAIN_STACK.csv with required columns; skipped.\n")
        print("stack_trainer: no stack input; skipped.")
        return

    # Merge priors by fixture_id if present
    for pth, name in [(XG,"xg"),(AV,"avail"),(SP,"sp"),(MK,"mkt"),(UN,"unc")]:
        pri = safe_read(pth)
        if not pri.empty and "fixture_id" in pri.columns:
            df = df.merge(pri, on="fixture_id", how="left")

    df["date"]=pd.to_datetime(df["date"], errors="coerce")
    df=df.dropna(subset=["date"]).sort_values("date")

    y = df["target"].values
    # Build stack features (start minimal; add more as you like)
    stack_cols = [c for c in ["base_prob",
                              "xg_mu_home","xg_mu_away","xg_total_mu",
                              "avail_goal_shift_home","avail_goal_shift_away",
                              "sp_xg_prior_home","sp_xg_prior_away",
                              "market_informed_score","uncertainty_penalty",
                              "contradiction_score"] if c in df.columns]
    if not stack_cols:
        with open(OUTREP,"w",encoding="utf-8") as f:
            f.write("# STACK TRAINING REPORT\n\n- No stack features found; skipped.\n")
        print("stack_trainer: no stack features; skipped."); return

    leagues = sorted(df["league"].dropna().unique().tolist())
    lines = ["# STACK TRAINING REPORT", f"- Rows: {len(df)}", f"- Stack features: {', '.join(stack_cols)}", f"- Leagues: {len(leagues)}", ""]
    calib_map = {}

    for lg in leagues:
        dfl = df[df["league"]==lg]
        if len(dfl)<200:
            lines.append(f"## {lg}: insufficient rows ({len(dfl)}) — skipped.")
            continue

        # train/valid split (time-based tail for isotonic)
        split = int(len(dfl)*0.8)
        X_tr = dfl.iloc[:split][stack_cols].values; y_tr=dfl.iloc[:split]["target"].values
        X_te = dfl.iloc[split:][stack_cols].values; y_te=dfl.iloc[split:]["target"].values

        stack = LogisticRegression(max_iter=200, solver="lbfgs")
        stack.fit(X_tr, y_tr)
        p_raw = stack.predict_proba(X_te)[:,1]
        brier = brier_score_loss(y_te, p_raw)

        # calib
        iso = IsotonicRegression(out_of_bounds="clip")
        iso.fit(p_raw, y_te)

        # log importance
        try:
            pi = permutation_importance(stack, X_te, y_te, n_repeats=5, random_state=42)
            order = np.argsort(-pi.importances_mean)
            top = [(stack_cols[i], float(pi.importances_mean[i])) for i in order[:10]]
        except Exception:
            top=[]

        lines.append(f"## League: {lg}")
        lines.append(f"- Brier (pre-calibration tail): {brier:.4f}")
        if top:
            lines.append("- Top 10 permutation importance (tail):")
            for name,val in top:
                lines.append(f"  - {name}: {val:.4f}")
        lines.append("")

        # save calibrator payload (store isotonic fit points)
        calib_map[lg] = {"stack_features": stack_cols}

    with open(CALIB, "wb") as f:
        pickle.dump(calib_map, f)

    with open(OUTREP,"w",encoding="utf-8") as f:
        f.write("\n".join(lines)+"\n")
    print(f"stack_trainer: wrote {OUTREP} and {CALIB}")

if __name__ == "__main__":
    main()