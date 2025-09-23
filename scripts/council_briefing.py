#!/usr/bin/env python3
import os, io
import pandas as pd
import numpy as np
from datetime import datetime

RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
OUT = os.path.join(RUN_DIR, "AUTO_BRIEFING.md")  # renamed output

# ---------- helpers ----------

def safe_read(path, cols=None):
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

def df_to_md(df):
    """to_markdown fallback (no tabulate needed)."""
    if df is None or len(df) == 0:
        return "_(no rows)_"
    try:
        return df.to_markdown(index=False)
    except Exception:
        cols = list(df.columns)
        lines = []
        lines.append("| " + " | ".join(cols) + " |")
        lines.append("| " + " | ".join(["---"] * len(cols)) + " |")
        for _, r in df.iterrows():
            vals = ["" if pd.isna(v) else str(v) for v in r.tolist()]
            lines.append("| " + " | ".join(vals) + " |")
        return "\n".join(lines)

def top_edges(preds, prob_col, stake_col="stake", n=5):
    df = preds.copy()
    if df.empty:
        return df
    if stake_col in df:
        df = df.sort_values(stake_col, ascending=False, kind="stable")
    elif prob_col in df:
        df = df.sort_values(prob_col, ascending=False, kind="stable")
    return df.head(n)

# ---------- main ----------

def main():
    # Load (robust if files are missing)
    p1x2 = safe_read(os.path.join(RUN_DIR, "PREDICTIONS_7D.csv"))
    btts = safe_read(os.path.join(RUN_DIR, "PREDICTIONS_BTTS_7D.csv"), ["fixture_id","league","p_btts_yes"])
    tot  = safe_read(os.path.join(RUN_DIR, "PREDICTIONS_TOTALS_7D.csv"), ["fixture_id","league","p_over","p_under"])
    act  = safe_read(os.path.join(RUN_DIR, "ACTIONABILITY_REPORT.csv"))
    cal  = safe_read(os.path.join(RUN_DIR, "CALIBRATION_SUMMARY.csv"))
    cons = safe_read(os.path.join(RUN_DIR, "CONSISTENCY_CHECKS.csv"))
    risk = safe_read(os.path.join(RUN_DIR, "EXECUTION_FEASIBILITY.csv"))
    idxj = safe_read(os.path.join(RUN_DIR, "_INDEX.json"))  # may not be CSV

    # Ensure columns we reference exist
    for col in ["fixture_id","league"]:
        for df in (p1x2, btts, tot, act, cons, risk):
            if col not in df.columns:
                df[col] = np.nan
    if "stake" not in act.columns:
        act["stake"] = 0.0
    if "pH" not in act.columns and "pH" in p1x2.columns:
        act = act.merge(p1x2[["fixture_id","league","pH","pD","pA"]], on=["fixture_id","league"], how="left")

    # Compose the document
    buf = io.StringIO()
    buf.write(f"# AUTO_BRIEFING — {datetime.utcnow().strftime('%Y-%m-%d')} UTC\n\n")
    buf.write("> **Disclaimer:** This is an **automated pipeline summary** for triage only.\n")
    buf.write("> The Council’s real briefing happens in Stages 5–7 — this file is **not binding**.\n\n")

    # Morning checklist (from _INDEX.json if present)
    buf.write("## Morning Checklist (auto)\n")
    try:
        # if _INDEX.json exists, show key items
        import json
        jpath = os.path.join(RUN_DIR, "_INDEX.json")
        if os.path.exists(jpath):
            J = json.load(open(jpath, "r"))
            keys = ["n_fixtures","n_edges","feasibility_pct","ece_weighted","consistency_flags","veto_slices","coverage_hint"]
            K = {k: J.get(k, None) for k in keys}
            df_chk = pd.DataFrame([K])
            buf.write(df_to_md(df_chk))
        else:
            buf.write("_No _INDEX.json available._")
    except Exception:
        buf.write("_Unable to parse _INDEX.json._")
    buf.write("\n\n")

    # Top edges
    buf.write("## Top Edges (by stake)\n")
    buf.write("**1X2**\n\n")
    t1 = top_edges(act, "pH", "stake", n=5)
    buf.write(df_to_md(t1[["fixture_id","league","stake","pH","pD","pA"]] if not t1.empty else t1))
    buf.write("\n\n**BTTS**\n\n")
    if not btts.empty:
        bt2 = act.merge(btts[["fixture_id","p_btts_yes"]], on="fixture_id", how="left")
        bt2 = bt2.sort_values("stake", ascending=False, kind="stable").head(5)
        cols = [c for c in ["fixture_id","league","p_btts_yes","stake"] if c in bt2.columns]
        buf.write(df_to_md(bt2[cols]))
    else:
        buf.write("_No BTTS predictions._\n")

    buf.write("\n\n**Totals**\n\n")
    if not tot.empty:
        to2 = act.merge(tot[["fixture_id","p_over"]], on="fixture_id", how="left")
        to2 = to2.sort_values("stake", ascending=False, kind="stable").head(5)
        cols = [c for c in ["fixture_id","league","p_over","stake"] if c in to2.columns]
        buf.write(df_to_md(to2[cols]))
    else:
        buf.write("_No Totals predictions._\n")

    # Calibration
    buf.write("\n\n## Calibration / ECE (per league)\n")
    if not cal.empty:
        buf.write(df_to_md(cal))
    else:
        buf.write("_Calibration summary not available this run._\n")

    # Consistency flags
    buf.write("\n\n## Consistency Flags\n")
    if not cons.empty:
        if "flag_goals_vs_totals" not in cons.columns: cons["flag_goals_vs_totals"] = 0
        if "flag_over_vs_btts" not in cons.columns: cons["flag_over_vs_btts"] = 0
        fl = cons[(cons["flag_goals_vs_totals"]==1) | (cons["flag_over_vs_btts"]==1)]
        if len(fl):
            cols = [c for c in ["fixture_id","league","flag_goals_vs_totals","flag_over_vs_btts"] if c in fl.columns]
            buf.write(df_to_md(fl[cols].head(20)))
        else:
            buf.write("_No major inconsistencies detected._\n")
    else:
        buf.write("_No consistency data._\n")

    # Risk / feasibility
    buf.write("\n\n## Risk / Feasibility\n")
    if not risk.empty:
        cols = [c for c in ["fixture_id","league","num_books","feasible","note"] if c in risk.columns]
        buf.write(df_to_md(risk[cols].head(20)))
    else:
        buf.write("_No feasibility data._\n")

    with open(OUT, "w") as f:
        f.write(buf.getvalue())
    print("AUTO_BRIEFING.md written:", OUT)

if __name__ == "__main__":
    main()