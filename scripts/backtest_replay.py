#!/usr/bin/env python3
"""
backtest_replay.py — Monthly offline backtests to stress-test priors, stacker, and risk controls.

Inputs (best-effort; safe with missing):
  data/TRAIN_MATRIX.csv        (date, league, target, [numeric features])
  data/TRAIN_STACK.csv         (date, league, target, base_prob, fixture_id, [contradiction_score])
  data/PRIORS_XG_SIM.csv       (fixture_id, xg_mu_home, xg_mu_away, xg_total_mu)
  data/PRIORS_AVAIL.csv        (fixture_id, avail_goal_shift_home, avail_goal_shift_away)
  data/PRIORS_SETPIECE.csv     (fixture_id, sp_xg_prior_home, sp_xg_prior_away)
  data/PRIORS_MKT.csv          (fixture_id, market_informed_score)
  data/PRIORS_UNC.csv          (fixture_id, uncertainty_penalty)
  data/leagues_allowlist.csv   (league, liquidity_tier, max_units)   # optional for risk replay

Outputs:
  data/BACKTEST_BY_WEEK.csv    (week_end, league, n, prob_mean, hit_rate, brier, ece_bin, ece_weighted, pnl_units)
  data/BACKTEST_SUMMARY.csv    (league, weeks, n, brier, ece_weighted, pnl_units)
  reports/REPLAY_REPORT.md     (human summary: metrics, P&L proxy, stress notes)

Notes:
- If TRAIN_STACK.csv is present: we replay a simple **stack** feature set (base_prob + priors + contradiction_score).
- Otherwise: we replay using TRAIN_MATRIX.csv as a base head (single classifier).
- Risk P&L proxy uses Kelly suggestions as "final_stake" * outcome (optional; simplified).
- All logic is time-aware (rolling-origin) to avoid look-ahead.
"""

import os, numpy as np, pandas as pd
from datetime import datetime
from sklearn.linear_model import LogisticRegression
from sklearn.isotonic import IsotonicRegression

DATA="data"; REP="reports"
os.makedirs(REP, exist_ok=True)

MTX = os.path.join(DATA,"TRAIN_MATRIX.csv")
STK = os.path.join(DATA,"TRAIN_STACK.csv")
XG  = os.path.join(DATA,"PRIORS_XG_SIM.csv")
AV  = os.path.join(DATA,"PRIORS_AVAIL.csv")
SP  = os.path.join(DATA,"PRIORS_SETPIECE.csv")
MK  = os.path.join(DATA,"PRIORS_MKT.csv")
UN  = os.path.join(DATA,"PRIORS_UNC.csv")
ALLOW = os.path.join(DATA,"leagues_allowlist.csv")

OUT_W = os.path.join(DATA,"BACKTEST_BY_WEEK.csv")
OUT_S = os.path.join(DATA,"BACKTEST_SUMMARY.csv")
OUT_R = os.path.join(REP,"REPLAY_REPORT.md")

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

def weekly_end(d):
    # align to week end (Sunday) for grouping
    # pandas dt.weekday: Monday=0 ... Sunday=6; week_end = d + (6 - wd)
    wd = d.weekday()
    return d + pd.Timedelta(days=(6 - wd))

def risk_pnl_proxy(prob, y, kelly=1.0, tier=1, cap=1.0):
    """Simplified Kelly PnL proxy: stake = min(kelly * tier_scale, cap), PnL = stake*(2*y-1) [symmetric].
       This is a toy; replace with your actual odds PnL if available.
    """
    tier_scale = {1:1.0, 2:0.85, 3:0.70}.get(int(tier), 1.0)
    stake = min(kelly * tier_scale, cap)
    return stake * (2*y - 1)

def main():
    df_mtx = safe_read(MTX)
    df_stk = safe_read(STK)

    if df_mtx.empty and df_stk.empty:
        pd.DataFrame(columns=["week_end","league","n","prob_mean","hit_rate","brier","ece_bin","ece_weighted","pnl_units"]).to_csv(OUT_W, index=False)
        pd.DataFrame(columns=["league","weeks","n","brier","ece_weighted","pnl_units"]).to_csv(OUT_S, index=False)
        with open(OUT_R,"w",encoding="utf-8") as f:
            f.write("# REPLAY REPORT\n\n- No training matrices present; replay skipped.\n")
        print("backtest_replay: no TRAIN_MATRIX or TRAIN_STACK; skipped."); return

    # Base time axis
    if not df_stk.empty:
        df = df_stk.copy()
    else:
        df = df_mtx.copy()

    # sanitize
    if "date" not in df.columns or "league" not in df.columns or "target" not in df.columns:
        pd.DataFrame(columns=["week_end","league","n","prob_mean","hit_rate","brier","ece_bin","ece_weighted","pnl_units"]).to_csv(OUT_W, index=False)
        pd.DataFrame(columns=["league","weeks","n","brier","ece_weighted","pnl_units"]).to_csv(OUT_S, index=False)
        with open(OUT_R,"w",encoding="utf-8") as f:
            f.write("# REPLAY REPORT\n\n- TRAIN matrix missing date/league/target columns; replay skipped.\n")
        print("backtest_replay: matrix missing essential columns; skipped."); return

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")

    # Merge priors by fixture_id where available
    for pth in [XG, AV, SP, MK, UN]:
        pri = safe_read(pth)
        if not pri.empty and "fixture_id" in pri.columns and "fixture_id" in df.columns:
            df = df.merge(pri, on="fixture_id", how="left")

    allow = safe_read(ALLOW)
    tier_map, cap_map = {}, {}
    if not allow.empty:
        if "league" in allow.columns and "liquidity_tier" in allow.columns:
            tier_map = dict(zip(allow["league"], allow["liquidity_tier"]))
        if "league" in allow.columns and "max_units" in allow.columns:
            cap_map = dict(zip(allow["league"], allow["max_units"]))

    leagues = sorted(df["league"].dropna().unique().tolist())
    rows_week = []

    # Replay loop per league
    for lg in leagues:
        dfl = df[df["league"]==lg].copy()
        if len(dfl) < 200:
            continue

        # Define rolling splits (quarterly-ish)
        dates = dfl["date"].sort_values().unique()
        split1 = dates[int(len(dates)*0.6)]
        split2 = dates[int(len(dates)*0.8)]

        # Choose features
        drop_cols = {"fixture_id","target","date","league","home_team","away_team"}
        if not df_stk.empty and "base_prob" in dfl.columns:
            # Stacker replay: base_prob + priors + optional contradiction_score
            stack_cols = [c for c in ["base_prob","xg_mu_home","xg_mu_away","xg_total_mu",
                                      "avail_goal_shift_home","avail_goal_shift_away",
                                      "sp_xg_prior_home","sp_xg_prior_away",
                                      "market_informed_score","uncertainty_penalty",
                                      "contradiction_score"] if c in dfl.columns]
            if not stack_cols:
                continue
            # fit on split1, validate on (split1, split2], test tail > split2
            train_mask = dfl["date"] <= split1
            valid_mask = (dfl["date"] > split1) & (dfl["date"] <= split2)
            test_mask  = dfl["date"] > split2

            model = LogisticRegression(max_iter=200, solver="lbfgs")
            if train_mask.sum() < 100 or test_mask.sum() < 50:
                continue
            model.fit(dfl.loc[train_mask, stack_cols], dfl.loc[train_mask, "target"])
            # Calibrate with validation tail
            if valid_mask.sum() >= 50:
                val_p = model.predict_proba(dfl.loc[valid_mask, stack_cols])[:,1]
                val_y = dfl.loc[valid_mask, "target"].values
                iso = IsotonicRegression(out_of_bounds="clip")
                iso.fit(val_p, val_y)
                p_test_raw = model.predict_proba(dfl.loc[test_mask, stack_cols])[:,1]
                p_test = iso.transform(p_test_raw)
            else:
                p_test = model.predict_proba(dfl.loc[test_mask, stack_cols])[:,1]
            dfl.loc[test_mask,"prob_replay"] = p_test

        else:
            # Base head replay from TRAIN_MATRIX.csv
            Xcols = [c for c in dfl.columns if c not in drop_cols and str(dfl[c].dtype).startswith(("float","int"))]
            if not Xcols:
                continue
            train_mask = dfl["date"] <= split1
            valid_mask = (dfl["date"] > split1) & (dfl["date"] <= split2)
            test_mask  = dfl["date"] > split2

            model = LogisticRegression(max_iter=200, solver="lbfgs")
            if train_mask.sum() < 100 or test_mask.sum() < 50:
                continue
            model.fit(dfl.loc[train_mask, Xcols], dfl.loc[train_mask, "target"])
            if valid_mask.sum() >= 50:
                val_p = model.predict_proba(dfl.loc[valid_mask, Xcols])[:,1]
                val_y = dfl.loc[valid_mask, "target"].values
                iso = IsotonicRegression(out_of_bounds="clip")
                iso.fit(val_p, val_y)
                p_test_raw = model.predict_proba(dfl.loc[test_mask, Xcols])[:,1]
                p_test = iso.transform(p_test_raw)
            else:
                p_test = model.predict_proba(dfl.loc[test_mask, Xcols])[:,1]
            dfl.loc[test_mask,"prob_replay"] = p_test

        # Compute weekly metrics on test tail
        tail = dfl[dfl["prob_replay"].notna()].copy()
        if tail.empty:
            continue

        tail["week_end"] = tail["date"].dt.date.apply(lambda d: weekly_end(pd.Timestamp(d))).astype(str)
        # Simple PnL proxy using per-league tier/cap and Kelly=1 unit baseline
        tier = int(tier_map.get(lg, 1))
        cap  = float(cap_map.get(lg, 1.0))
        # NOTE: in real PnL you’d use actual odds; here we keep a symmetric proxy
        pnl = tail.apply(lambda r: risk_pnl_proxy(r["prob_replay"], r["target"], 1.0, tier, cap), axis=1)
        tail["pnl_units"] = pnl

        # ECE per bin (weekly weighted)
        def ece_weighted(p,y):
            if len(p)==0: return np.nan
            # compute per-bin, then weighted
            bins = np.linspace(0,1,11)
            idx  = np.digitize(p, bins, right=True)
            total = len(p)
            acc=0.0
            for b in range(1,12):
                m = idx==b
                if m.any():
                    acc += abs(p[m].mean() - y[m].mean()) * (m.sum()/total)
            return acc

        by_week = tail.groupby("week_end").apply(
            lambda g: pd.Series({
                "n": len(g),
                "prob_mean": g["prob_replay"].mean(),
                "hit_rate": g["target"].mean(),
                "brier": ((g["prob_replay"]-g["target"])**2).mean(),
                "ece_weighted": ece_weighted(g["prob_replay"].values, g["target"].values),
                "pnl_units": g["pnl_units"].sum()
            })
        ).reset_index()
        by_week["league"] = lg
        rows_week.append(by_week)

    if rows_week:
        bw = pd.concat(rows_week, axis=0, ignore_index=True)
        bw = bw[["week_end","league","n","prob_mean","hit_rate","brier","ece_weighted","pnl_units"]]
        bw.to_csv(OUT_W, index=False)
        # Summary
        summ = bw.groupby("league").agg(
            weeks=("week_end","nunique"),
            n=("n","sum"),
            brier=("brier","mean"),
            ece_weighted=("ece_weighted","mean"),
            pnl_units=("pnl_units","sum")
        ).reset_index()
        summ.to_csv(OUT_S, index=False)
        # Human report
        lines = ["# REPLAY REPORT", f"_Generated: {datetime.utcnow().isoformat()}Z_", ""]
        for _, r in summ.iterrows():
            lines += [f"## {r['league']}",
                      f"- Weeks: {int(r['weeks'])}, Obs: {int(r['n'])}",
                      f"- Brier: {r['brier']:.4f}, ECE (weighted): {r['ece_weighted']:.4f}",
                      f"- PnL (proxy) units: {r['pnl_units']:.2f}", ""]
        with open(OUT_R, "w", encoding="utf-8") as f:
            f.write("\n".join(lines)+"\n")
        print(f"backtest_replay: wrote {OUT_W}, {OUT_S}, {OUT_R}")
    else:
        pd.DataFrame(columns=["week_end","league","n","prob_mean","hit_rate","brier","ece_weighted","pnl_units"]).to_csv(OUT_W, index=False)
        pd.DataFrame(columns=["league","weeks","n","brier","ece_weighted","pnl_units"]).to_csv(OUT_S, index=False)
        with open(OUT_R,"w",encoding="utf-8") as f:
            f.write("# REPLAY REPORT\n\n- No leagues had enough rows for rolling replay.\n")
        print("backtest_replay: no eligible leagues; wrote empty outputs.")

if __name__ == "__main__":
    main()