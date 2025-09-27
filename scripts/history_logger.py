#!/usr/bin/env python3
"""
history_logger.py — append run-time history rows for audit & movement tracking.

What it logs (per fixture_id):
- Run metadata: run_timestamp (UTC ISO), github_run_id (if available), workflow (if available)
- Core keys: date, league, home_team, away_team
- Odds snapshot: home_odds_dec, draw_odds_dec, away_odds_dec (if present), has_opening_odds, has_closing_odds,
                 ou_main_total, spread_home_line, spread_away_line, (and line-moves if present: ou_move, h2h_*_move, spread_*_move)
- Priors: xg_mu_home/away/total, availability shifts, set-piece priors, market_informed_score, uncertainty_penalty
- Model features (selected): spi_rank_diff, injury_index_diff, availability_diff, sca/gca/pressures/tackles/setpiece diffs (if present)
- Risk: final_stake and reasons from ACTIONABILITY_REPORT (merged by fixture_id)

Files read:
  data/UPCOMING_fixtures.csv
  data/UPCOMING_7D_enriched.csv
  data/UPCOMING_7D_features.csv
  data/ACTIONABILITY_REPORT.csv
  data/PRIORS_XG_SIM.csv
  data/PRIORS_AVAIL.csv
  data/PRIORS_SETPIECE.csv
  data/PRIORS_MKT.csv
  data/PRIORS_UNC.csv

Appends to:
  data/HISTORY_LOG.csv   (created if missing)

Safe behavior:
- If any source file is missing, continues with available columns.
- Deduplicates on (run_timestamp, fixture_id) to avoid double-append within the same run.
"""

import os, pandas as pd, numpy as np
from datetime import datetime

DATA = "data"
OUT  = os.path.join(DATA, "HISTORY_LOG.csv")

def safe_read(p):
    if not os.path.exists(p): return pd.DataFrame()
    try: return pd.read_csv(p)
    except Exception: return pd.DataFrame()

def ensure_fixture_id(df):
    if "fixture_id" in df.columns: return df
    if {"date","home_team","away_team"}.issubset(df.columns):
        def mk_id(r):
            d=str(r.get("date","NA")).replace("-","").replace("T","_").replace(":","")
            h=str(r.get("home_team","NA")).strip().lower().replace(" ","_")
            a=str(r.get("away_team","NA")).strip().lower().replace(" ","_")
            return f"{d}__{h}__vs__{a}"
        df["fixture_id"] = df.apply(mk_id, axis=1)
    return df

def take(df, cols):
    """Return df with subset of columns if they exist."""
    return df[[c for c in cols if c in df.columns]] if not df.empty else pd.DataFrame(columns=[c for c in cols])

def main():
    run_ts  = datetime.utcnow().isoformat()+"Z"
    run_id  = os.environ.get("GITHUB_RUN_ID","")
    wf_name = os.environ.get("GITHUB_WORKFLOW","data-fetch-preflight")

    fix  = ensure_fixture_id(safe_read(os.path.join(DATA,"UPCOMING_fixtures.csv")))
    enr  = ensure_fixture_id(safe_read(os.path.join(DATA,"UPCOMING_7D_enriched.csv")))
    feat = ensure_fixture_id(safe_read(os.path.join(DATA,"UPCOMING_7D_features.csv")))
    act  = safe_read(os.path.join(DATA,"ACTIONABILITY_REPORT.csv"))
    xg   = safe_read(os.path.join(DATA,"PRIORS_XG_SIM.csv"))
    av   = safe_read(os.path.join(DATA,"PRIORS_AVAIL.csv"))
    sp   = safe_read(os.path.join(DATA,"PRIORS_SETPIECE.csv"))
    mkt  = safe_read(os.path.join(DATA,"PRIORS_MKT.csv"))
    unc  = safe_read(os.path.join(DATA,"PRIORS_UNC.csv"))

    # core join frame
    base_cols = ["fixture_id","date","league","home_team","away_team"]
    core = take(ensure_fixture_id(fix if not fix.empty else enr), base_cols)

    if core.empty:
        # nothing to log – write header if missing and exit quietly
        if not os.path.exists(OUT):
            pd.DataFrame(columns=["run_timestamp","github_run_id","workflow"]+base_cols).to_csv(OUT, index=False)
        print("history_logger: no core rows; nothing appended.")
        return

    # enrich columns to log
    odds_cols = [
        "home_odds_dec","draw_odds_dec","away_odds_dec",
        "has_opening_odds","has_closing_odds",
        "ou_main_total","spread_home_line","spread_away_line",
        "ou_move","h2h_home_move","h2h_draw_move","h2h_away_move",
        "spread_home_line_move","spread_away_line_move"
    ]
    core = core.merge(take(enr, ["fixture_id"]+odds_cols), on="fixture_id", how="left")

    # priors
    core = core.merge(take(xg,  ["fixture_id","xg_mu_home","xg_mu_away","xg_total_mu"]), on="fixture_id", how="left")
    core = core.merge(take(av,  ["fixture_id","avail_goal_shift_home","avail_goal_shift_away"]), on="fixture_id", how="left")
    core = core.merge(take(sp,  ["fixture_id","sp_xg_prior_home","sp_xg_prior_away"]), on="fixture_id", how="left")
    core = core.merge(take(mkt, ["fixture_id","market_informed_score"]), on="fixture_id", how="left")
    core = core.merge(take(unc, ["fixture_id","uncertainty_penalty"]), on="fixture_id", how="left")

    # selected feature diffs (optional – log what exists)
    feat_keep = [
        "spi_rank_diff","injury_index_diff","availability_diff",
        "keeper_psxg_prevented_diff","passing_accuracy_diff",
        "sca90_diff","gca90_diff","pressures90_diff","tackles90_diff","setpiece_share_diff",
        "rank_x_injury","avail_x_travel","unc_x_ou"
    ]
    core = core.merge(take(feat, ["fixture_id"]+feat_keep), on="fixture_id", how="left")

    # risk – stakes and reasons
    act_keep = ["fixture_id","final_stake","reasons"]
    core = core.merge(take(act, act_keep), on="fixture_id", how="left")

    # add run metadata
    core.insert(0, "run_timestamp", run_ts)
    core.insert(1, "github_run_id", run_id)
    core.insert(2, "workflow", wf_name)

    # append (dedupe against same run_timestamp+fixture_id)
    if os.path.exists(OUT):
        hist = pd.read_csv(OUT)
        key = ["run_timestamp","fixture_id"]
        merged = pd.concat([hist, core], axis=0, ignore_index=True)
        merged = merged.drop_duplicates(subset=key, keep="last")
        merged.to_csv(OUT, index=False)
    else:
        core.to_csv(OUT, index=False)

    print(f"history_logger: appended {len(core)} rows → {OUT}")

if __name__ == "__main__":
    main()