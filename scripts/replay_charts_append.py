#!/usr/bin/env python3
"""
replay_charts_append.py — Append model vs. market charts to REPLAY_REPORT.md

Reads:
  data/BACKTEST_BY_WEEK.csv  (from backtest_replay.py)

Writes (appends to):
  reports/REPLAY_REPORT.md

What it adds:
- For each league × bet_type, an ASCII table by **prob_bin** comparing models (base/stack/market):
    n, hit_rate, brier, ECE, ROI, PnL units
- For 1X2 when odds are present, a second table by **odds_bin**.

Safe: if input is missing/empty, it appends a short note and exits.
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime

DATA = "data"
REP  = "reports"
BW   = os.path.join(DATA, "BACKTEST_BY_WEEK.csv")
OUT  = os.path.join(REP,  "REPLAY_REPORT.md")

def safe_read(p):
    if not os.path.exists(p): return pd.DataFrame()
    try:
        return pd.read_csv(p)
    except Exception:
        return pd.DataFrame()

def fmt(x, spec=".3f"):
    """
    Format numbers safely with a dynamic format spec (e.g., '.3f').
    Returns 'N/A' for NaN/None and preserves strings/bins as-is.
    """
    try:
        if pd.isna(x):
            return "N/A"
    except Exception:
        pass
    try:
        v = float(x)
        return f"{v:{spec}}"
    except Exception:
        return str(x)

def table_by(df, by_cols, title, sort_cols=None):
    if df.empty:
        return [f"\n### {title}\n- (no data)\n"]

    # aggregate
    df2 = df.groupby(by_cols + ["model"], dropna=False).agg(
        n=("n","sum"),
        hit_rate=("hit_rate","mean"),
        brier=("brier","mean"),
        ece_weighted=("ece_weighted","mean"),
        roi=("roi","mean"),
        pnl_units=("pnl_units","sum"),
    ).reset_index()

    if sort_cols:
        # only sort on columns that exist
        sort_cols = [c for c in sort_cols if c in df2.columns]
        if sort_cols:
            df2 = df2.sort_values(sort_cols)

    # Render simple ASCII table
    lines = [f"\n### {title}", ""]
    headers = [c for c in by_cols if c in df2.columns] + [c for c in ["model","n","hit_rate","brier","ece_weighted","roi","pnl_units"] if c in df2.columns]
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("|" + "|".join(["---"]*len(headers)) + "|")

    for _, r in df2.iterrows():
        row = []
        for c in headers:
            v = r[c]
            if c in ("hit_rate","brier","ece_weighted","roi"):
                row.append(fmt(v, ".3f"))
            elif c == "pnl_units":
                row.append(fmt(v, ".2f"))
            else:
                row.append("N/A" if (isinstance(v, float) and pd.isna(v)) else str(v))
        lines.append("| " + " | ".join(row) + " |")
    lines.append("")
    return lines

def main():
    os.makedirs(REP, exist_ok=True)
    bw = safe_read(BW)

    if bw.empty or not {"league","bet_type","model","n","hit_rate","brier","ece_weighted","pnl_units"}.issubset(bw.columns):
        with open(OUT, "a", encoding="utf-8") as f:
            f.write("\n---\n## Charts\n- (No BACKTEST_BY_WEEK.csv data to chart)\n")
        print("replay_charts_append: no data to chart; appended note.")
        return

    # Ensure prob_bin & odds_bin exist (may be all-NaN in some models)
    if "prob_bin" not in bw.columns: bw["prob_bin"] = np.nan
    if "odds_bin" not in bw.columns: bw["odds_bin"] = np.nan

    leagues = sorted(bw["league"].dropna().unique().tolist())
    with open(OUT, "a", encoding="utf-8") as f:
        f.write("\n---\n## Charts (Model vs Market by Bins)\n")
        f.write(f"_Generated: {datetime.utcnow().isoformat()}Z_\n")

        for lg in leagues:
            bw_lg = bw[bw["league"]==lg]
            if bw_lg.empty:
                continue
            f.write(f"\n\n## {lg}\n")

            for bet in ["1X2","OU","BTTS"]:
                seg = bw_lg[bw_lg["bet_type"]==bet]
                if seg.empty:
                    continue

                # Table by prob bin (always)
                lines = table_by(
                    seg,
                    by_cols=["prob_bin"],
                    title=f"{bet} — by prob_bin",
                    sort_cols=["prob_bin","model"]
                )
                f.write("\n".join(lines))

                # Table by odds bin (1X2 only, where present)
                if bet == "1X2" and seg["odds_bin"].notna().any():
                    lines = table_by(
                        seg[seg["odds_bin"].notna()],
                        by_cols=["odds_bin"],
                        title=f"{bet} — by odds_bin",
                        sort_cols=["odds_bin","model"]
                    )
                    f.write("\n".join(lines))

    print(f"replay_charts_append: appended charts to {OUT}")

if __name__ == "__main__":
    main()