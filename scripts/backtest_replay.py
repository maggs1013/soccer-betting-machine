#!/usr/bin/env python3
"""
backtest_replay.py — Monthly offline backtests to stress-test priors, stacker, and risk controls.

EXTENDED (compared to prior version):
- Slices by LEAGUE, BET TYPE (1X2, OU, BTTS), MODEL (base/stack/market)
- Odds bins & prob bins
- Market baseline (from implied probabilities if odds are available)
- ROI/EV using odds when available; proxy PnL otherwise
- Preserves rolling-origin train/valid/test; per-league time-aware splits

Inputs (best-effort; safe with missing):
  data/TRAIN_MATRIX.csv        (date, league, target, [numeric features], optional: bet_type, fixture_id)
  data/TRAIN_STACK.csv         (date, league, target, base_prob, fixture_id, [contradiction_score], optional: bet_type)
  data/PRIORS_XG_SIM.csv       (fixture_id, xg_mu_home, xg_mu_away, xg_total_mu)
  data/PRIORS_AVAIL.csv        (fixture_id, avail_goal_shift_home, avail_goal_shift_away)
  data/PRIORS_SETPIECE.csv     (fixture_id, sp_xg_prior_home, sp_xg_prior_away)
  data/PRIORS_MKT.csv          (fixture_id, market_informed_score)
  data/PRIORS_UNC.csv          (fixture_id, uncertainty_penalty)
  data/leagues_allowlist.csv   (league, liquidity_tier, max_units)   # optional for risk replay
  data/UPCOMING_fixtures.csv   (optional historical odds snapshot: home_odds_dec, draw_odds_dec, away_odds_dec, etc.)

Outputs:
  data/BACKTEST_BY_WEEK.csv    (week_end, league, bet_type, model, odds_bin, prob_bin, n, hit_rate, brier, ece_weighted, roi, pnl_units)
  data/BACKTEST_SUMMARY.csv    (league, bet_type, model, weeks, n, brier, ece_weighted, roi, pnl_units)
  reports/REPLAY_REPORT.md     (human summary)

Notes:
- If TRAIN_STACK.csv is present, we evaluate a simple “stack” feature set;
  otherwise we evaluate a base head from TRAIN_MATRIX.csv.
- Market baseline is computed from implied probabilities (if odds available).
- EV/ROI is approximate unless your training data includes actual matched odds per outcome;
  we’ve implemented a best-effort using available columns.
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
FIX   = os.path.join(DATA,"UPCOMING_fixtures.csv")

OUT_W = os.path.join(DATA,"BACKTEST_BY_WEEK.csv")
OUT_S = os.path.join(DATA,"BACKTEST_SUMMARY.csv")
OUT_R = os.path.join(REP,"REPLAY_REPORT.md")

def safe_read(p):
    if not os.path.exists(p): return pd.DataFrame()
    try: return pd.read_csv(p)
    except Exception: return pd.DataFrame()

def ece_weighted(p,y):
    if len(p)==0: return np.nan
    bins = np.linspace(0,1,11)
    idx  = np.digitize(p, bins, right=True)
    total = len(p)
    acc=0.0
    for b in range(1,12):
        m = idx==b
        if m.any():
            acc += abs(p[m].mean() - y[m].mean()) * (m.sum()/total)
    return acc

def weekly_end(d):
    wd = d.weekday()
    return d + pd.Timedelta(days=(6 - wd))

def implied_prob_from_decimal(price):
    try:
        price = float(price)
        return 1.0/price if price>0 else np.nan
    except Exception:
        return np.nan

def add_odds_bins(df, prob_col="prob", price_col=None):
    df["prob_bin"] = pd.cut(df[prob_col], bins=[0,0.2,0.4,0.6,0.8,1.0], include_lowest=True)
    if price_col and price_col in df.columns:
        # price bins: <=1.75 (odds-on), 1.75-2.5, 2.5-4.0, >4.0
        df["odds_bin"] = pd.cut(pd.to_numeric(df[price_col], errors="coerce"),
                                bins=[0,1.75,2.5,4.0,999], include_lowest=True)
    else:
        df["odds_bin"] = pd.Categorical([np.nan]*len(df))
    return df

def roi_from_probs_and_odds(y, pick_prob, dec_price):
    """
    ROI per bet (single-outcome) with decimal odds: EV = p*(price-1) - (1-p)
    y: 0/1 actual; we also compute realized PnL if desired.
    """
    p = pd.to_numeric(pick_prob, errors="coerce")
    pr = pd.to_numeric(dec_price, errors="coerce")
    ev = p*(pr-1.0) - (1.0-p)
    # realized pnl if we 'bet' 1 unit:
    realized = (pr-1.0)*y - (1-y)
    return ev, realized

def risk_pnl_proxy(prob, y, kelly=1.0, tier=1, cap=1.0):
    tier_scale = {1:1.0, 2:0.85, 3:0.70}.get(int(tier), 1.0)
    stake = min(kelly * tier_scale, cap)
    return stake * (2*y - 1)

def train_calibrated_logit(X_tr, y_tr, X_iso, y_iso):
    model = LogisticRegression(max_iter=200, solver="lbfgs")
    model.fit(X_tr, y_tr)
    if len(y_iso) >= 50:
        iso = IsotonicRegression(out_of_bounds="clip")
        p_iso_in = model.predict_proba(X_iso)[:,1]
        iso.fit(p_iso_in, y_iso)
        return model, iso
    return model, None

def predict_calibrated(model, iso, X):
    p = model.predict_proba(X)[:,1]
    if iso is not None:
        try:
            p = iso.transform(p)
        except Exception:
            pass
    return p

def main():
    df_mtx = safe_read(MTX)
    df_stk = safe_read(STK)
    if df_mtx.empty and df_stk.empty:
        pd.DataFrame(columns=["week_end","league","bet_type","model","odds_bin","prob_bin","n","hit_rate","brier","ece_weighted","roi","pnl_units"]).to_csv(OUT_W, index=False)
        pd.DataFrame(columns=["league","bet_type","model","weeks","n","brier","ece_weighted","roi","pnl_units"]).to_csv(OUT_S, index=False)
        with open(OUT_R,"w",encoding="utf-8") as f:
            f.write("# REPLAY REPORT\n\n- No training matrices present; replay skipped.\n")
        print("backtest_replay: no TRAIN_MATRIX or TRAIN_STACK; skipped."); return

    # Prefer stack file if present for "stack" model; base always available from TRAIN_MATRIX if present
    have_stack = not df_stk.empty
    have_base  = not df_mtx.empty

    # Ensure required columns
    def sanitize(df):
        if "date" not in df.columns or "league" not in df.columns or "target" not in df.columns:
            return pd.DataFrame()
        df["date"]=pd.to_datetime(df["date"], errors="coerce")
        df=df.dropna(subset=["date"]).sort_values("date")
        if "bet_type" not in df.columns:
            df["bet_type"] = "1X2"
        return df

    df_mtx = sanitize(df_mtx)
    df_stk = sanitize(df_stk)

    if df_mtx.empty and df_stk.empty:
        pd.DataFrame(columns=["week_end","league","bet_type","model","odds_bin","prob_bin","n","hit_rate","brier","ece_weighted","roi","pnl_units"]).to_csv(OUT_W, index=False)
        pd.DataFrame(columns=["league","bet_type","model","weeks","n","brier","ece_weighted","roi","pnl_units"]).to_csv(OUT_S, index=False)
        with open(OUT_R,"w",encoding="utf-8") as f:
            f.write("# REPLAY REPORT\n\n- Training matrices missing essential columns; replay skipped.\n")
        print("backtest_replay: matrices missing essential columns; skipped."); return

    # Merge priors by fixture_id where available (use df_mtx if present, else df_stk)
    def merge_priors(df):
        for pth in [XG, AV, SP, MK, UN]:
            pri = safe_read(pth)
            if not pri.empty and "fixture_id" in pri.columns and "fixture_id" in df.columns:
                df = df.merge(pri, on="fixture_id", how="left")
        return df

    if have_base: df_mtx = merge_priors(df_mtx)
    if have_stack: df_stk = merge_priors(df_stk)

    # Market baseline preparation: implied probs from odds if available
    fx = safe_read(FIX)
    if not fx.empty:
        # standard H2H implied
        for col in ["home_odds_dec","draw_odds_dec","away_odds_dec"]:
            if col in fx.columns:
                fx[f"imp_{col}"] = fx[col].apply(implied_prob_from_decimal)
        # optional attach by fixture_id if present
        if "fixture_id" not in fx.columns and "date" in fx.columns and "home_team" in fx.columns and "away_team" in fx.columns:
            def mk_id(r):
                d=str(r.get("date","NA")).replace("-","").replace("T","_").replace(":","")
                h=str(r.get("home_team","NA")).strip().lower().replace(" ","_")
                a=str(r.get("away_team","NA")).strip().lower().replace(" ","_")
                return f"{d}__{h}__vs__{a}"
            fx["fixture_id"]=fx.apply(mk_id,axis=1)

    allow = safe_read(ALLOW)
    tier_map, cap_map = {}, {}
    if not allow.empty:
        if "league" in allow.columns and "liquidity_tier" in allow.columns:
            tier_map = dict(zip(allow["league"], allow["liquidity_tier"]))
        if "league" in allow.columns and "max_units" in allow.columns:
            cap_map = dict(zip(allow["league"], allow["max_units"]))

    # Assemble models to evaluate
    MODEL_LIST = []
    if have_base:  MODEL_LIST.append("base")
    if have_stack: MODEL_LIST.append("stack")
    # market baseline only if odds available & bet type supported
    MARKET_OK = not fx.empty

    rows_week = []

    # Evaluate per league and bet_type
    leagues = sorted(set((df_mtx["league"] if have_base else pd.Series(dtype=str)).tolist() +
                         (df_stk["league"] if have_stack else pd.Series(dtype=str)).tolist()))
    for lg in leagues:
        for bet in ["1X2","OU","BTTS"]:
            # Slice training data for this league/bet_type
            mtx_lg = df_mtx[(df_mtx["league"]==lg) & (df_mtx["bet_type"]==bet)] if have_base else pd.DataFrame()
            stk_lg = df_stk[(df_stk["league"]==lg) & (df_stk["bet_type"]==bet)] if have_stack else pd.DataFrame()
            if mtx_lg.empty and stk_lg.empty:
                continue

            # Build timeline from whichever has data
            base_df = stk_lg if not stk_lg.empty else mtx_lg
            dates = base_df["date"].sort_values().unique()
            if len(dates) < 200:
                continue

            # Split points (60/20/20)
            split1 = dates[int(len(dates)*0.6)]
            split2 = dates[int(len(dates)*0.8)]

            # Market baseline setup (1X2 only unless you carry OU/BTTS odds)
            fx_lg = pd.DataFrame()
            if MARKET_OK and "fixture_id" in base_df.columns and "fixture_id" in fx.columns:
                fx_lg = fx[["fixture_id"] + [c for c in fx.columns if c.startswith("imp_") or c.endswith("_odds_dec")]].copy()

            # Evaluate each model
            for model_name in MODEL_LIST + (["market"] if MARKET_OK else []):
                if model_name=="base" and mtx_lg.empty:    continue
                if model_name=="stack" and stk_lg.empty:   continue
                use_df = {"base": mtx_lg, "stack": stk_lg}.get(model_name, base_df).copy()

                # Choose features
                drop_cols = {"fixture_id","target","date","league","home_team","away_team","bet_type"}
                if model_name=="stack":
                    stack_cols = [c for c in ["base_prob","xg_mu_home","xg_mu_away","xg_total_mu",
                                              "avail_goal_shift_home","avail_goal_shift_away",
                                              "sp_xg_prior_home","sp_xg_prior_away",
                                              "market_informed_score","uncertainty_penalty",
                                              "contradiction_score"] if c in use_df.columns]
                    if not stack_cols: 
                        continue
                    train_mask = use_df["date"] <= split1
                    valid_mask = (use_df["date"] > split1) & (use_df["date"] <= split2)
                    test_mask  = use_df["date"] > split2
                    if train_mask.sum() < 100 or test_mask.sum() < 50:
                        continue
                    model, iso = train_calibrated_logit(use_df.loc[train_mask, stack_cols].values,
                                                        use_df.loc[train_mask,"target"].values,
                                                        use_df.loc[valid_mask, stack_cols].values,
                                                        use_df.loc[valid_mask,"target"].values)
                    p_test = predict_calibrated(model, iso, use_df.loc[test_mask, stack_cols].values)
                    tail = use_df.loc[test_mask, ["date","target","fixture_id"]].copy()
                    tail["prob"] = p_test

                elif model_name=="base":
                    Xcols = [c for c in use_df.columns if c not in drop_cols and str(use_df[c].dtype).startswith(("float","int"))]
                    if not Xcols:
                        continue
                    train_mask = use_df["date"] <= split1
                    valid_mask = (use_df["date"] > split1) & (use_df["date"] <= split2)
                    test_mask  = use_df["date"] > split2
                    if train_mask.sum() < 100 or test_mask.sum() < 50:
                        continue
                    model, iso = train_calibrated_logit(use_df.loc[train_mask, Xcols].values,
                                                        use_df.loc[train_mask,"target"].values,
                                                        use_df.loc[valid_mask, Xcols].values,
                                                        use_df.loc[valid_mask,"target"].values)
                    p_test = predict_calibrated(model, iso, use_df.loc[test_mask, Xcols].values)
                    tail = use_df.loc[test_mask, ["date","target","fixture_id"]].copy()
                    tail["prob"] = p_test

                else:  # market baseline
                    if fx_lg.empty:
                        continue
                    # Attach odds & implied probs; compute baseline prob
                    tail = base_df[base_df["date"] > split2][["date","target","fixture_id"]].copy()
                    tail = tail.merge(fx_lg, on="fixture_id", how="left")
                    # For 1X2 only: pick a single-outcome prob baseline (home win probability)
                    if bet=="1X2":
                        tail["prob"] = tail.get("imp_home_odds_dec", np.nan)
                        # If implied not present, skip market baseline for this bet type/league
                        if tail["prob"].isna().all():
                            continue
                    else:
                        # OU/BTTS baseline requires recorded odds for those markets; skip if absent
                        continue

                # optional odds merge for ROI calc (1X2 home price only as demo)
                # You can extend to full 3-way or OU/BTTS once you store those odds in TRAIN_* or a historical odds table
                if not fx_lg.empty:
                    tail = tail.merge(fx_lg, on="fixture_id", how="left")

                # Compute weekly metrics & ROI/EV
                tail = tail.dropna(subset=["prob"]).copy()
                if tail.empty:
                    continue
                tail["date"] = pd.to_datetime(tail["date"], errors="coerce")
                tail = tail.dropna(subset=["date"])
                tail["week_end"] = tail["date"].dt.date.apply(lambda d: weekly_end(pd.Timestamp(d))).astype(str)

                # EV/ROI and PnL
                if bet=="1X2" and "away_odds_dec" in tail.columns and "home_odds_dec" in tail.columns:
                    # Assume we're evaluating 'home' pick for demo; for full system, evaluate per predicted pick
                    ev, realized = roi_from_probs_and_odds(tail["target"].values, tail["prob"].values, tail["home_odds_dec"].values)
                    tail["roi"] = ev
                    tail["pnl_units"] = realized
                    price_col = "home_odds_dec"
                else:
                    # fallback proxy if odds missing or bet type not supported
                    lg_tier = int((tier_map.get(lg, 1)))
                    lg_cap  = float((cap_map.get(lg, 1.0)))
                    tail["pnl_units"] = tail.apply(lambda r: risk_pnl_proxy(r["prob"], r["target"], 1.0, lg_tier, lg_cap), axis=1)
                    tail["roi"] = np.nan
                    price_col = None

                tail = add_odds_bins(tail, prob_col="prob", price_col=price_col)

                # Aggregate by week x odds_bin x prob_bin
                grp = tail.groupby(["week_end","odds_bin","prob_bin"], dropna=False)
                agg = grp.apply(lambda g: pd.Series({
                    "n": len(g),
                    "hit_rate": g["target"].mean(),
                    "brier": ((g["prob"]-g["target"])**2).mean(),
                    "ece_weighted": ece_weighted(g["prob"].values, g["target"].values),
                    "roi": g["roi"].mean(skipna=True) if "roi" in g.columns else np.nan,
                    "pnl_units": g["pnl_units"].sum()
                })).reset_index()
                agg["league"]   = lg
                agg["bet_type"] = bet
                agg["model"]    = model_name
                rows_week.append(agg)

    # Write outputs
    if rows_week:
        BW = pd.concat(rows_week, axis=0, ignore_index=True)
        # order columns
        BW = BW[["week_end","league","bet_type","model","odds_bin","prob_bin","n","hit_rate","brier","ece_weighted","roi","pnl_units"]]
        BW.to_csv(OUT_W, index=False)

        # Summary per league x bet_type x model
        SUMM = BW.groupby(["league","bet_type","model"], dropna=False).agg(
            weeks=("week_end","nunique"),
            n=("n","sum"),
            brier=("brier","mean"),
            ece_weighted=("ece_weighted","mean"),
            roi=("roi","mean"),
            pnl_units=("pnl_units","sum")
        ).reset_index()
        SUMM.to_csv(OUT_S, index=False)

        # Human report
        lines = ["# REPLAY REPORT", f"_Generated: {datetime.utcnow().isoformat()}Z_", ""]
        for _, r in SUMM.sort_values(["league","bet_type","model"]).iterrows():
            lines += [f"## {r['league']} — {r['bet_type']} — {r['model']}",
                      f"- Weeks: {int(r['weeks'])}, Obs: {int(r['n'])}",
                      f"- Brier: {r['brier']:.4f}, ECE (weighted): {r['ece_weighted']:.4f}",
                      f"- ROI (avg): {('%.3f'%r['roi']) if pd.notna(r['roi']) else 'N/A'}",
                      f"- PnL units (proxy or realized): {r['pnl_units']:.2f}", ""]
        with open(OUT_R, "w", encoding="utf-8") as f:
            f.write("\n".join(lines)+"\n")
        print(f"backtest_replay: wrote {OUT_W}, {OUT_S}, {OUT_R}")
    else:
        pd.DataFrame(columns=["week_end","league","bet_type","model","odds_bin","prob_bin","n","hit_rate","brier","ece_weighted","roi","pnl_units"]).to_csv(OUT_W, index=False)
        pd.DataFrame(columns=["league","bet_type","model","weeks","n","brier","ece_weighted","roi","pnl_units"]).to_csv(OUT_S, index=False)
        with open(OUT_R,"w",encoding="utf-8") as f:
            f.write("# REPLAY REPORT\n\n- No eligible slices for replay (insufficient rows or missing columns).\n")
        print("backtest_replay: no eligible slices; wrote empty outputs.")

if __name__ == "__main__":
    main()