#!/usr/bin/env python3
"""
history_logger.py — append run-time history rows for audit & movement tracking.

What it logs (per fixture_id):
- Run metadata: run_timestamp (UTC ISO), github_run_id (if available), workflow (if available)
- Core keys: date, league, home_team, away_team
- Odds snapshot: home_odds_dec, draw_odds_dec, away_odds_dec (if present),
                 has_opening_odds, has_closing_odds,
                 ou_main_total, spread_home_line, spread_away_line,
                 (and line-moves if present: ou_move, h2h_*_move, spread_*_move)
- Priors: xg_mu_home/away/total, availability shifts, set-piece priors,
          market_informed_score, uncertainty_penalty
- Features (selected diffs): spi_rank_diff, injury_index_diff, availability_diff,
          keeper_psxg_prevented_diff, passing_accuracy_diff, sca90_diff, gca90_diff,
          pressures90_diff, tackles90_diff, setpiece_share_diff,
          rank_x_injury, avail_x_travel, unc_x_ou
- Risk: final_stake and reasons from ACTIONABILITY_REPORT
- OPTIONAL taps (safe if missing):
    * Model outputs: prob, selection, kelly, bet_type (from PREDICTIONS_7D.csv)
    * League risk context: liquidity_tier, max_units (from leagues_allowlist.csv)
    * Contradictions: contradiction_count (from CONSISTENCY_CHECKS.csv)
    * Priors coverage flag: priors_coverage_all ∈ {0,1}

Files read (safe with missing):
  data/UPCOMING_fixtures.csv
  data/UPCOMING_7D_enriched.csv
  data/UPCOMING_7D_features.csv
  data/ACTIONABILITY_REPORT.csv
  data/PRIORS_XG_SIM.csv
  data/PRIORS_AVAIL.csv
  data/PRIORS_SETPIECE.csv
  data/PRIORS_MKT.csv
  data/PRIORS_UNC.csv
  data/PREDICTIONS_7D.csv                 (optional)
  data/leagues_allowlist.csv              (optional)
  data/CONSISTENCY_CHECKS.csv             (optional)

Appends to:
  data/HISTORY_LOG.csv   (created if missing)

Safe behavior:
- Continues with available columns if any source file is missing.
- Deduplicates on (run_timestamp, fixture_id) to avoid double-append within the same run.
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime

DATA = "data"
OUT  = os.path.join(DATA, "HISTORY_LOG.csv")

def safe_read(path: str) -> pd.DataFrame:
    if not os.path.exists(path): return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()

def ensure_fixture_id(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    if "fixture_id" in df.columns: return df
    need = {"date","home_team","away_team"}
    if need.issubset(df.columns):
        def mk_id(r):
            d=str(r.get("date","NA")).replace("-","").replace("T","_").replace(":","")
            h=str(r.get("home_team","NA")).strip().lower().replace(" ","_")
            a=str(r.get("away_team","NA")).strip().lower().replace(" ","_")
            return f"{d}__{h}__vs__{a}"
        df = df.copy()
        df["fixture_id"] = df.apply(mk_id, axis=1)
    return df

def take(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    """Return a frame with only requested columns that exist (or empty with those headers)."""
    if df.empty:
        return pd.DataFrame(columns=[c for c in cols])
    keep = [c for c in cols if c in df.columns]
    if not keep:
        return pd.DataFrame(columns=[c for c in cols])
    return df[keep]

def main():
    # Run metadata
    run_ts  = datetime.utcnow().isoformat() + "Z"
    run_id  = os.environ.get("GITHUB_RUN_ID", "")
    wf_name = os.environ.get("GITHUB_WORKFLOW", "data-fetch-preflight")

    # Core sources
    fix  = ensure_fixture_id(safe_read(os.path.join(DATA,"UPCOMING_fixtures.csv")))
    enr  = ensure_fixture_id(safe_read(os.path.join(DATA,"UPCOMING_7D_enriched.csv")))
    feat = ensure_fixture_id(safe_read(os.path.join(DATA,"UPCOMING_7D_features.csv")))
    act  = safe_read(os.path.join(DATA,"ACTIONABILITY_REPORT.csv"))

    # Priors
    xg   = safe_read(os.path.join(DATA,"PRIORS_XG_SIM.csv"))
    av   = safe_read(os.path.join(DATA,"PRIORS_AVAIL.csv"))
    sp   = safe_read(os.path.join(DATA,"PRIORS_SETPIECE.csv"))
    mktp = safe_read(os.path.join(DATA,"PRIORS_MKT.csv"))
    unc  = safe_read(os.path.join(DATA,"PRIORS_UNC.csv"))

    # Optional taps
    preds = safe_read(os.path.join(DATA,"PREDICTIONS_7D.csv"))
    allow = safe_read(os.path.join(DATA,"leagues_allowlist.csv"))
    cons  = safe_read(os.path.join(DATA,"CONSISTENCY_CHECKS.csv"))

    # Core join frame (prefer fixtures; fallback to enriched for keys)
    base_cols = ["fixture_id","date","league","home_team","away_team"]
    core_src  = fix if not fix.empty else enr
    core      = take(ensure_fixture_id(core_src), base_cols)

    if core.empty:
        # Write header once and exit quietly
        if not os.path.exists(OUT):
            pd.DataFrame(columns=["run_timestamp","github_run_id","workflow"] + base_cols).to_csv(OUT, index=False)
        print("history_logger: no core rows; nothing appended.")
        return

    # Odds snapshot + line-moves from enrichment
    odds_cols = [
        "home_odds_dec","draw_odds_dec","away_odds_dec",
        "has_opening_odds","has_closing_odds",
        "ou_main_total","spread_home_line","spread_away_line",
        "ou_move","h2h_home_move","h2h_draw_move","h2h_away_move",
        "spread_home_line_move","spread_away_line_move"
    ]
    core = core.merge(take(enr, ["fixture_id"] + odds_cols), on="fixture_id", how="left")

    # Priors (merge by fixture_id)
    core = core.merge(take(xg,  ["fixture_id","xg_mu_home","xg_mu_away","xg_total_mu"]), on="fixture_id", how="left")
    core = core.merge(take(av,  ["fixture_id","avail_goal_shift_home","avail_goal_shift_away"]), on="fixture_id", how="left")
    core = core.merge(take(sp,  ["fixture_id","sp_xg_prior_home","sp_xg_prior_away"]), on="fixture_id", how="left")
    core = core.merge(take(mktp,["fixture_id","market_informed_score"]), on="fixture_id", how="left")
    core = core.merge(take(unc, ["fixture_id","uncertainty_penalty"]), on="fixture_id", how="left")

    # Priors coverage flag (1 if all five present on this fixture)
    def pri_flag(row):
        fields = [
            row.get("xg_mu_home"), row.get("xg_mu_away"),
            row.get("avail_goal_shift_home"), row.get("avail_goal_shift_away"),
            row.get("sp_xg_prior_home"), row.get("sp_xg_prior_away"),
            row.get("market_informed_score"), row.get("uncertainty_penalty")
        ]
        return int(all(pd.notna(fields)))
    core["priors_coverage_all"] = core.apply(pri_flag, axis=1)

    # Selected feature diffs
    feat_keep = [
        "spi_rank_diff","injury_index_diff","availability_diff",
        "keeper_psxg_prevented_diff","passing_accuracy_diff",
        "sca90_diff","gca90_diff","pressures90_diff","tackles90_diff","setpiece_share_diff",
        "rank_x_injury","avail_x_travel","unc_x_ou"
    ]
    core = core.merge(take(feat, ["fixture_id"] + feat_keep), on="fixture_id", how="left")

    # Risk (stakes + reasons)
    act_keep = ["fixture_id","final_stake","reasons"]
    core = core.merge(take(act, act_keep), on="fixture_id", how="left")

    # Optional model outputs (prob, selection, kelly, bet_type)
    if not preds.empty:
        pred_keep = []
        for c in ["fixture_id","prob","selection","kelly","bet_type"]:
            if c in preds.columns: pred_keep.append(c)
        if pred_keep:
            core = core.merge(preds[pred_keep].drop_duplicates("fixture_id", keep="last"),
                              on="fixture_id", how="left")

    # Optional contradictions count
    if not cons.empty and {"fixture_id","check"}.issubset(cons.columns):
        ccount = cons.groupby("fixture_id")["check"].count().reset_index().rename(columns={"check":"contradiction_count"})
        core = core.merge(ccount, on="fixture_id", how="left")
    else:
        core["contradiction_count"] = np.nan

    # Optional league risk context (liquidity tiers + caps)
    if not allow.empty and {"league","liquidity_tier","max_units"}.issubset(allow.columns) and "league" in core.columns:
        core = core.merge(allow[["league","liquidity_tier","max_units"]].drop_duplicates("league"),
                          on="league", how="left")
    else:
        core["liquidity_tier"] = np.nan
        core["max_units"] = np.nan

    # Add run metadata
    core.insert(0, "run_timestamp", run_ts)
    core.insert(1, "github_run_id", run_id)
    core.insert(2, "workflow", wf_name)

    # Append (dedupe against same run_timestamp+fixture_id)
    if os.path.exists(OUT):
        hist = pd.read_csv(OUT)
        merged = pd.concat([hist, core], axis=0, ignore_index=True)
        merged = merged.drop_duplicates(subset=["run_timestamp","fixture_id"], keep="last")
        merged.to_csv(OUT, index=False)
    else:
        core.to_csv(OUT, index=False)

    print(f"history_logger: appended {len(core)} rows → {OUT}")

if __name__ == "__main__":
    main()