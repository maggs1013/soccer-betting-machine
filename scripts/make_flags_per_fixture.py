#!/usr/bin/env python3
"""
make_flags_per_fixture.py
Build a single fixture-level flags sheet for quick triage.

Output:
  runs/YYYY-MM-DD/FLAGS.csv

Columns:
  fixture_id, league, stake, cap_scale?, consistency_flag, feasible, note, league_veto_present
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime

RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
os.makedirs(RUN_DIR, exist_ok=True)

def safe_read(path, cols=None):
    if not os.path.exists(path):
        return pd.DataFrame(columns=cols or [])
    try:
        df = pd.read_csv(path)
        if cols:
            for c in cols:
                if c not in df.columns: df[c] = np.nan
        return df
    except Exception:
        return pd.DataFrame(columns=cols or [])

def main():
    act  = safe_read(os.path.join(RUN_DIR, "ACTIONABILITY_REPORT.csv"))
    cons = safe_read(os.path.join(RUN_DIR, "CONSISTENCY_CHECKS.csv"))
    feas = safe_read(os.path.join(RUN_DIR, "EXECUTION_FEASIBILITY.csv"))
    veto = safe_read(os.path.join(RUN_DIR, "ANTI_MODEL_VETOES.csv"))
    pred = safe_read(os.path.join(RUN_DIR, "PREDICTIONS_7D.csv"))

    # base rows from predictions (so every fixture appears)
    base_cols = ["fixture_id","league"]
    base = pred[base_cols].drop_duplicates() if not pred.empty else pd.DataFrame(columns=base_cols)

    # add stake & cap_scale from act if present
    if not act.empty:
        keep = ["fixture_id","league"]
        for c in ("stake","cap_scale"):
            if c in act.columns: keep.append(c)
        act2 = act[keep].copy()
        base = base.merge(act2, on=["fixture_id","league"], how="left")
    else:
        base["stake"] = np.nan
        base["cap_scale"] = np.nan

    # consistency flags
    if not cons.empty:
        cons2 = cons[["fixture_id","flag_goals_vs_totals","flag_over_vs_btts"]].copy() if set(["flag_goals_vs_totals","flag_over_vs_btts"]).issubset(cons.columns) else pd.DataFrame()
        if not cons2.empty:
            cons2["consistency_flag"] = cons2[["flag_goals_vs_totals","flag_over_vs_btts"]].fillna(0).astype(int).max(axis=1)
            cons2 = cons2[["fixture_id","consistency_flag"]]
            base = base.merge(cons2, on="fixture_id", how="left")
    if "consistency_flag" not in base.columns:
        base["consistency_flag"] = 0

    # feasibility
    if not feas.empty:
        feas2 = feas.drop_duplicates("fixture_id")
        keep = ["fixture_id"]
        for c in ("feasible","note","num_books"):
            if c in feas2.columns: keep.append(c)
        base = base.merge(feas2[keep], on="fixture_id", how="left")
    else:
        base["feasible"] = np.nan
        base["note"] = np.nan
        base["num_books"] = np.nan

    # league veto present (slice-based, not fixture-specific)
    if not veto.empty and "league" in base.columns:
        veto_leagues = set()
        if "slice" in veto.columns:
            # parse league from "League :: odds_bucket=.."
            for s in veto["slice"].dropna().astype(str):
                lg = s.split("::")[0].strip()
                if lg: veto_leagues.add(lg)
        # fallback: no slice string? try direct league column
        if not veto_leagues and "league" in veto.columns:
            veto_leagues = set(veto["league"].dropna().astype(str).unique())
        base["league_veto_present"] = base["league"].astype(str).apply(lambda lg: 1 if lg in veto_leagues else 0)
    else:
        base["league_veto_present"] = 0

    out = base[["fixture_id","league","stake","cap_scale","consistency_flag","feasible","num_books","note","league_veto_present"]]
    out.to_csv(os.path.join(RUN_DIR, "FLAGS.csv"), index=False)
    print("[OK] FLAGS.csv written")

if __name__ == "__main__":
    main()