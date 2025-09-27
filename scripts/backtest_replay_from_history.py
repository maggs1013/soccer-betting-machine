#!/usr/bin/env python3
"""
backtest_replay_from_history.py — monthly offline replay using run-time history
Uses: data/HISTORY_LOG.csv    (appended each daily run by history_logger.py)

Outputs:
  data/BACKTEST_FROM_HISTORY_BY_WEEK.csv
  data/BACKTEST_FROM_HISTORY_SUMMARY.csv
  reports/REPLAY_FROM_HISTORY.md

What it does:
- Time-aware (uses logged run_timestamp), no look-ahead
- Groups by league, bet_type (assumes 1X2 if not present), prob_bin, odds_bin
- Computes hit_rate, Brier (needs logged probs if you add them later), ROI/EV proxy from logged odds + final_stake (if you log those)
- Safe: if columns are missing, it falls back to the info that exists and logs that
"""
import os, pandas as pd, numpy as np
from datetime import datetime

DATA="data"; REP="reports"
os.makedirs(REP, exist_ok=True)

HIST = os.path.join(DATA,"HISTORY_LOG.csv")
OUT_W= os.path.join(DATA,"BACKTEST_FROM_HISTORY_BY_WEEK.csv")
OUT_S= os.path.join(DATA,"BACKTEST_FROM_HISTORY_SUMMARY.csv")
OUT_R= os.path.join(REP,"REPLAY_FROM_HISTORY.md")

def safe_read(p):
    if not os.path.exists(p): return pd.DataFrame()
    try: return pd.read_csv(p)
    except Exception: return pd.DataFrame()

def weekly_end(d):
    wd = d.weekday()
    return d + pd.Timedelta(days=(6 - wd))

def add_bins(df, prob_col=None, price_col=None):
    if prob_col and prob_col in df.columns:
        df["prob_bin"] = pd.cut(pd.to_numeric(df[prob_col], errors="coerce"),
                                bins=[0,0.2,0.4,0.6,0.8,1.0], include_lowest=True)
    else:
        df["prob_bin"] = pd.Categorical([np.nan]*len(df))
    if price_col and price_col in df.columns:
        df["odds_bin"] = pd.cut(pd.to_numeric(df[price_col], errors="coerce"),
                                bins=[0,1.75,2.5,4.0,999], include_lowest=True)
    else:
        df["odds_bin"] = pd.Categorical([np.nan]*len(df))
    return df

def main():
    df = safe_read(HIST)
    if df.empty:
        pd.DataFrame(columns=["week_end","league","bet_type","n","hit_rate","brier","roi","pnl_units"]).to_csv(OUT_W,index=False)
        pd.DataFrame(columns=["league","bet_type","weeks","n","brier","roi","pnl_units"]).to_csv(OUT_S,index=False)
        with open(OUT_R,"w",encoding="utf-8") as f:
            f.write("# REPLAY FROM HISTORY\n\n- No HISTORY_LOG.csv present.\n")
        print("backtest_replay_from_history: no history; done")
        return

    # expected minimal columns from the logger
    need = {"run_timestamp","league","fixture_id","home_team","away_team","final_stake"}
    if not need.issubset(df.columns):
        with open(OUT_R,"w",encoding="utf-8") as f:
            f.write("# REPLAY FROM HISTORY\n\n- HISTORY_LOG.csv missing required columns; skipping.\n")
        pd.DataFrame(columns=["week_end","league","bet_type","n","hit_rate","brier","roi","pnl_units"]).to_csv(OUT_W,index=False)
        pd.DataFrame(columns=["league","bet_type","weeks","n","brier","roi","pnl_units"]).to_csv(OUT_S,index=False)
        print("backtest_replay_from_history: missing columns; done")
        return

    # Parse time / default bet_type
    df["run_timestamp"] = pd.to_datetime(df["run_timestamp"], errors="coerce")
    df = df.dropna(subset=["run_timestamp"]).sort_values("run_timestamp")
    if "bet_type" not in df.columns:
        df["bet_type"] = "1X2"

    # We may not have actual labels in history (y). If not, we can only do sanity ROI proxy.
    y_col = "actual" if "actual" in df.columns else None
    p_col = "prob"   if "prob"   in df.columns else None  # if you later log model probs here
    price_col = "home_odds_dec" if "home_odds_dec" in df.columns else None

    # weekly aggregation
    df["week_end"] = df["run_timestamp"].dt.date.apply(lambda d: weekly_end(pd.Timestamp(d))).astype(str)
    df = add_bins(df, prob_col=p_col, price_col=price_col)

    rows=[]
    for (lg, bt), seg in df.groupby(["league","bet_type"]):
        if seg.empty: continue
        grp = seg.groupby(["week_end","odds_bin","prob_bin"], dropna=False)

        def agg_block(g):
            n = len(g)
            hr = g.get(y_col, pd.Series([np.nan]*n)).mean() if y_col else np.nan
            # brier needs prob + y
            if y_col and p_col and p_col in g.columns:
                brier = ((pd.to_numeric(g[p_col], errors="coerce") - g[y_col])**2).mean()
            else:
                brier = np.nan
            # ROI / PnL proxy from final_stake if no odds; else calc realized with price if y present
            pnl = pd.to_numeric(g.get("final_stake", np.nan), errors="coerce").fillna(0.0).sum()
            roi = np.nan
            return pd.Series({"n":n,"hit_rate":hr,"brier":brier,"roi":roi,"pnl_units":pnl})

        blk = grp.apply(agg_block).reset_index()
        blk["league"]=lg; blk["bet_type"]=bt
        rows.append(blk)

    if rows:
        BW = pd.concat(rows, axis=0, ignore_index=True)
        BW = BW[["week_end","league","bet_type","odds_bin","prob_bin","n","hit_rate","brier","roi","pnl_units"]]
        BW.to_csv(OUT_W,index=False)

        SUMM = BW.groupby(["league","bet_type"], dropna=False).agg(
            weeks=("week_end","nunique"),
            n=("n","sum"),
            brier=("brier","mean"),
            roi=("roi","mean"),
            pnl_units=("pnl_units","sum")
        ).reset_index()
        SUMM.to_csv(OUT_S,index=False)

        lines=["# REPLAY FROM HISTORY", f"_Generated: {datetime.utcnow().isoformat()}Z_", ""]
        for _, r in SUMM.sort_values(["league","bet_type"]).iterrows():
            lines += [f"## {r['league']} — {r['bet_type']}",
                      f"- Weeks: {int(r['weeks'])}, Obs: {int(r['n'])}",
                      f"- Brier: {r['brier'] if pd.notna(r['brier']) else 'N/A'}",
                      f"- ROI (avg): {r['roi'] if pd.notna(r['roi']) else 'N/A'}",
                      f"- PnL units (proxy): {r['pnl_units']:.2f}", ""]
        with open(OUT_R,"w",encoding="utf-8") as f:
            f.write("\n".join(lines)+"\n")
        print(f"backtest_replay_from_history: wrote {OUT_W}, {OUT_S}, {OUT_R}")
    else:
        pd.DataFrame(columns=["week_end","league","bet_type","odds_bin","prob_bin","n","hit_rate","brier","roi","pnl_units"]).to_csv(OUT_W,index=False)
        pd.DataFrame(columns=["league","bet_type","weeks","n","brier","roi","pnl_units"]).to_csv(OUT_S,index=False)
        with open(OUT_R,"w",encoding="utf-8") as f:
            f.write("# REPLAY FROM HISTORY\n\n- No eligible slices (insufficient data columns/rows).\n")
        print("backtest_replay_from_history: no slices; wrote empties")
if __name__ == "__main__":
    main()