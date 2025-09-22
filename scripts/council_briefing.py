#!/usr/bin/env python3
import os, io
import pandas as pd
from datetime import datetime

RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
OUT = os.path.join(RUN_DIR, "COUNCIL_BRIEFING.md")

def top_edges(preds, prob_col, stake_col="stake", n=5):
    df = preds.copy()
    if stake_col in df:
        df = df.sort_values(stake_col, ascending=False)
    else:
        df = df.sort_values(prob_col, ascending=False)
    return df.head(n)

def main():
    # Load
    p1x2 = pd.read_csv(os.path.join(RUN_DIR, "PREDICTIONS_7D.csv"))
    btts = pd.read_csv(os.path.join(RUN_DIR, "PREDICTIONS_BTTS_7D.csv"))
    tot  = pd.read_csv(os.path.join(RUN_DIR, "PREDICTIONS_TOTALS_7D.csv"))
    act  = pd.read_csv(os.path.join(RUN_DIR, "ACTIONABILITY_REPORT.csv"))
    cal  = pd.read_csv(os.path.join(RUN_DIR, "CALIBRATION_SUMMARY.csv")) if os.path.exists(os.path.join(RUN_DIR,"CALIBRATION_SUMMARY.csv")) else pd.DataFrame()
    cons = pd.read_csv(os.path.join(RUN_DIR, "CONSISTENCY_CHECKS.csv"))
    risk = pd.read_csv(os.path.join(RUN_DIR, "EXECUTION_FEASIBILITY.csv"))

    # Compose
    buf = io.StringIO()
    buf.write(f"# COUNCIL BRIEFING — {datetime.utcnow().strftime('%Y-%m-%d')} UTC\n\n")
    buf.write("## Top Edges (by stake)\n")
    buf.write("**1X2**\n\n")
    buf.write(top_edges(act, "pH", "stake").to_markdown(index=False))
    buf.write("\n\n**BTTS**\n\n")
    if "p_btts_yes" in btts:
        bt2 = act.merge(btts[["fixture_id","p_btts_yes"]], on="fixture_id", how="left").sort_values("stake", ascending=False).head(5)
        buf.write(bt2[["fixture_id","league","p_btts_yes","stake"]].to_markdown(index=False))
    else:
        buf.write("_No BTTS predictions._\n")
    buf.write("\n\n**Totals**\n\n")
    if "p_over" in tot:
        to2 = act.merge(tot[["fixture_id","p_over"]], on="fixture_id", how="left").sort_values("stake", ascending=False).head(5)
        buf.write(to2[["fixture_id","league","p_over","stake"]].to_markdown(index=False))
    else:
        buf.write("_No Totals predictions._\n")

    # Calibration
    buf.write("\n\n## Calibration / ECE (per league)\n")
    if not cal.empty:
        buf.write(cal.to_markdown(index=False))
    else:
        buf.write("_Calibration summary not available this run._\n")

    # Consistency flags
    buf.write("\n\n## Consistency Flags\n")
    fl = cons[(cons["flag_goals_vs_totals"]==1) | (cons["flag_over_vs_btts"]==1)]
    if len(fl):
        buf.write(fl[["fixture_id","league","flag_goals_vs_totals","flag_over_vs_btts"]].head(20).to_markdown(index=False))
    else:
        buf.write("_No major inconsistencies detected._\n")

    # Risk / feasibility
    buf.write("\n\n## Risk / Feasibility\n")
    if len(risk):
        buf.write(risk.head(20)[["fixture_id","league","num_books","feasible","note"]].to_markdown(index=False))
    else:
        buf.write("_No feasibility data._\n")

    with open(OUT, "w") as f:
        f.write(buf.getvalue())
    print("COUNCIL_BRIEFING.md written.")

if __name__ == "__main__":
    main()