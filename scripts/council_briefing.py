#!/usr/bin/env python3
"""
council_briefing.py
Writes runs/YYYY-MM-DD/AUTO_BRIEFING.md — a non-binding, automated triage dashboard.

Shows:
  • Run KPIs from _INDEX.json (incl. ΔLogLoss(Blend−Mkt, last 8w))
  • Top edges by stake for 1X2 / BTTS / Totals
  • Calibration summary table
  • Consistency flags
  • Feasibility snapshot

NOTE: This file is not the Council’s human briefing. It’s only a machine summary.
"""

import os, io, json
import pandas as pd
import numpy as np
from datetime import datetime

RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
OUT = os.path.join(RUN_DIR, "AUTO_BRIEFING.md")

def safe_read_csv(path, cols=None):
    if not os.path.exists(path):
        return pd.DataFrame(columns=cols or [])
    try:
        df = pd.read_csv(path)
        if cols:
            for c in cols:
                if c not in df.columns:
                    df[c] = np.nan
        return df
    except Exception:
        return pd.DataFrame(columns=cols or [])

def df_to_md(df: pd.DataFrame) -> str:
    if df is None or len(df) == 0:
        return "_(no rows)_"
    try:
        return df.to_markdown(index=False)
    except Exception:
        cols = list(df.columns)
        if not cols:
            return "_(no rows)_"
        lines = []
        lines.append("| " + " | ".join(cols) + " |")
        lines.append("| " + " | ".join(["---"] * len(cols)) + " |")
        for _, r in df.iterrows():
            vals = ["" if pd.isna(v) else str(v) for v in r.tolist()]
            lines.append("| " + " | ".join(vals) + " |")
        return "\n".join(lines)

def top_edges(preds: pd.DataFrame, prob_col: str, stake_col: str = "stake", n: int = 5) -> pd.DataFrame:
    df = preds.copy()
    if df.empty:
        return df
    if stake_col in df.columns:
        df = df.sort_values(stake_col, ascending=False, kind="stable")
    elif prob_col in df.columns:
        df = df.sort_values(prob_col, ascending=False, kind="stable")
    return df.head(n)

def main():
    # Core artifacts
    p1x2 = safe_read_csv(os.path.join(RUN_DIR, "PREDICTIONS_7D.csv"))
    btts = safe_read_csv(os.path.join(RUN_DIR, "PREDICTIONS_BTTS_7D.csv"),
                         cols=["fixture_id","league","p_btts_yes"])
    tot  = safe_read_csv(os.path.join(RUN_DIR, "PREDICTIONS_TOTALS_7D.csv"),
                         cols=["fixture_id","league","p_over","p_under"])
    act  = safe_read_csv(os.path.join(RUN_DIR, "ACTIONABILITY_REPORT.csv"))
    cal  = safe_read_csv(os.path.join(RUN_DIR, "CALIBRATION_SUMMARY.csv"))
    cons = safe_read_csv(os.path.join(RUN_DIR, "CONSISTENCY_CHECKS.csv"))
    feas = safe_read_csv(os.path.join(RUN_DIR, "EXECUTION_FEASIBILITY.csv"))

    # Ensure keys exist where referenced
    for col in ["fixture_id","league"]:
        for df in (p1x2, btts, tot, act, cons, feas):
            if col not in df.columns:
                df[col] = np.nan
    if "stake" not in act.columns:
        act["stake"] = 0.0
    if ("pH" not in act.columns) and (not p1x2.empty) and set(["pH","pD","pA","fixture_id","league"]).issubset(p1x2.columns):
        act = act.merge(p1x2[["fixture_id","league","pH","pD","pA"]], on=["fixture_id","league"], how="left")

    # KPI line from _INDEX.json
    kpi_line = "_No _INDEX.json available._"
    try:
        with open(os.path.join(RUN_DIR, "_INDEX.json"), "r") as f:
            idx = json.load(f)
        kpi_line = (
            f"Fixtures={idx.get('n_fixtures')}, "
            f"Edges={idx.get('n_edges')}, "
            f"Feasible%={idx.get('feasibility_pct')}, "
            f"ECE={idx.get('ece_weighted')}, "
            f"ΔLogLoss(Blend−Mkt, last8w)={idx.get('blend_vs_market_logloss_delta_last8w')}"
        )
    except Exception:
        pass

    # Compose briefing
    buf = io.StringIO()
    buf.write(f"# AUTO_BRIEFING — {datetime.utcnow().strftime('%Y-%m-%d')} UTC\n\n")
    buf.write("> **Disclaimer:** Automated triage only. Council’s real briefing (Stages 5–7) is not bound by this file.\n\n")
    buf.write(f"**Run KPIs:** {kpi_line}\n\n")

    # Top edges (1X2, BTTS, Totals)
    buf.write("## Top Edges (by stake)\n")
    buf.write("**1X2**\n\n")
    t1 = top_edges(act, "pH", "stake", n=5)
    if not t1.empty:
        cols = [c for c in ["fixture_id","league","stake","pH","pD","pA"] if c in t1.columns]
        buf.write(df_to_md(t1[cols]))
    else:
        buf.write("_(no rows)_")

    buf.write("\n\n**BTTS**\n\n")
    if not btts.empty and not act.empty:
        bt = act.merge(btts[["fixture_id","p_btts_yes"]], on="fixture_id", how="left") \
                .sort_values("stake", ascending=False, kind="stable").head(5)
        cols = [c for c in ["fixture_id","league","p_btts_yes","stake"] if c in bt.columns]
        buf.write(df_to_md(bt[cols]))
    else:
        buf.write("_(no rows)_")

    buf.write("\n\n**Totals**\n\n")
    if not tot.empty and not act.empty:
        to2 = act.merge(tot[["fixture_id","p_over"]], on="fixture_id", how="left") \
                 .sort_values("stake", ascending=False, kind="stable").head(5)
        cols = [c for c in ["fixture_id","league","p_over","stake"] if c in to2.columns]
        buf.write(df_to_md(to2[cols]))
    else:
        buf.write("_(no rows)_")

    # Calibration summary
    buf.write("\n\n## Calibration / ECE (per league)\n")
    buf.write(df_to_md(cal) if not cal.empty else "_Calibration summary not available._\n")

    # Consistency flags
    buf.write("\n\n## Consistency Flags\n")
    if not cons.empty:
        if "flag_goals_vs_totals" not in cons.columns: cons["flag_goals_vs_totals"] = 0
        if "flag_over_vs_btts"   not in cons.columns: cons["flag_over_vs_btts"]   = 0
        fl = cons[(cons["flag_goals_vs_totals"]==1) | (cons["flag_over_vs_btts"]==1)]
        if not fl.empty:
            cols = [c for c in ["fixture_id","league","flag_goals_vs_totals","flag_over_vs_btts"] if c in fl.columns]
            buf.write(df_to_md(fl[cols].head(20)))
        else:
            buf.write("_No major inconsistencies detected._\n")
    else:
        buf.write("_No consistency data._\n")

    # Feasibility snapshot
    buf.write("\n\n## Feasibility (liquidity)\n")
    if not feas.empty:
        cols = [c for c in ["fixture_id","league","num_books","feasible","note"] if c in feas.columns]
        buf.write(df_to_md(feas[cols].head(20)))
    else:
        buf.write("_No feasibility data._\n")

    # Write
    with open(OUT, "w") as f:
        f.write(buf.getvalue())
    print("AUTO_BRIEFING.md written:", OUT)

if __name__ == "__main__":
    main()