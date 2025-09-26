#!/usr/bin/env python3
"""
kelly_policy_update.py — risk-aware stake modulation (FINAL with max_units + optional halt)

Inputs:
  data/PREDICTIONS_7D.csv
  data/CONSISTENCY_CHECKS.csv
  data/UPCOMING_7D_enriched.csv
  data/leagues_allowlist.csv  (optional; columns: league, liquidity_tier in {1,2,3}, max_units [float])

Outputs:
  data/ACTIONABILITY_REPORT.csv

Stake policy:
  final_stake = base_kelly * factor
  factor starts at 1.0 and is adjusted multiplicatively by:
    A) market contradictions (BTTS_vs_OU)        → ×0.6
    B) high dispersion without timing (>=12)     → ×0.7
    C) OU vs SPI divergence                      → ×0.5
    D) liquidity tier (1→×1.00, 2→×0.85, 3→×0.70)
  Then capped per league:
    E) final_stake = min(final_stake, max_units)   (if max_units provided)
Optional:
  Set env RISK_HALT=1 to force final_stake = 0 for all rows (dry-run safety).
"""

import os
import numpy as np
import pandas as pd

DATA = "data"
PRED = os.path.join(DATA, "PREDICTIONS_7D.csv")
CONS = os.path.join(DATA, "CONSISTENCY_CHECKS.csv")
ENR  = os.path.join(DATA, "UPCOMING_7D_enriched.csv")
ALLOW= os.path.join(DATA, "leagues_allowlist.csv")
OUT  = os.path.join(DATA, "ACTIONABILITY_REPORT.csv")

def safe_read(path: str) -> pd.DataFrame:
    if not os.path.exists(path): return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()

def mk_id(r: pd.Series) -> str:
    d = str(r.get("date","NA")).replace("-", "").replace("T", "_").replace(":", "")
    h = str(r.get("home_team","NA")).strip().lower().replace(" ", "_")
    a = str(r.get("away_team","NA")).strip().lower().replace(" ", "_")
    return f"{d}__{h}__vs__{a}"

def main():
    pred  = safe_read(PRED)
    cons  = safe_read(CONS)
    enr   = safe_read(ENR)
    allow = safe_read(ALLOW)

    # Halt switch (optional)
    risk_halt = os.environ.get("RISK_HALT", "0").strip() == "1"

    # No predictions → header-only
    if pred.empty:
        pd.DataFrame(columns=["fixture_id","selection","league","base_kelly","stake_factor","final_stake","reasons"]).to_csv(OUT, index=False)
        print("kelly_policy_update: no predictions; wrote header-only")
        return

    # Ensure fixture_id
    if "fixture_id" not in pred.columns:
        pred["fixture_id"] = pred.apply(mk_id, axis=1)

    # Build lookups
    flag_sum = {}
    if not cons.empty and {"fixture_id","flag"}.issubset(cons.columns):
        flag_sum = cons.groupby("fixture_id")["flag"].sum().to_dict()

    disp_map, open_map, close_map, lg_map = {}, {}, {}, {}
    if not enr.empty and "fixture_id" in enr.columns:
        if "bookmaker_count"   in enr.columns: disp_map  = dict(zip(enr["fixture_id"], enr["bookmaker_count"]))
        if "has_opening_odds"  in enr.columns: open_map  = dict(zip(enr["fixture_id"], enr["has_opening_odds"]))
        if "has_closing_odds"  in enr.columns: close_map = dict(zip(enr["fixture_id"], enr["has_closing_odds"]))
        if "league"            in enr.columns: lg_map    = dict(zip(enr["fixture_id"], enr["league"]))

    tier_map, cap_map = {}, {}
    if not allow.empty and "league" in allow.columns:
        if "liquidity_tier" in allow.columns:
            tier_map = dict(zip(allow["league"], allow["liquidity_tier"]))
        if "max_units" in allow.columns:
            cap_map  = dict(zip(allow["league"], allow["max_units"]))

    rows = []
    for _, r in pred.iterrows():
        fid = r["fixture_id"]
        sel = r.get("selection") or r.get("pick") or "N/A"
        base = r.get("kelly", np.nan)
        if pd.isna(base): base = 1.0

        reasons = []
        factor  = 1.0

        # A) market contradictions
        if flag_sum.get(fid, 0) > 0:
            factor *= 0.6
            reasons.append("market_contradiction")

        # B) high dispersion without timing
        disp = disp_map.get(fid, np.nan)
        has_open  = int(open_map.get(fid, 0))  if fid in open_map  else 0
        has_close = int(close_map.get(fid, 0)) if fid in close_map else 0
        try:
            if pd.notna(disp) and float(disp) >= 12 and (not has_open) and (not has_close):
                factor *= 0.7
                reasons.append("high_dispersion_no_timing")
        except Exception:
            pass

        # C) OU vs SPI divergence
        if not cons.empty and {"fixture_id","check"}.issubset(cons.columns):
            sub = cons[(cons["fixture_id"] == fid) & (cons["check"] == "OU_vs_SPI")]
            if not sub.empty:
                factor *= 0.5
                reasons.append("ou_vs_spi_divergence")

        # D) Liquidity tier
        lg   = lg_map.get(fid, None)
        tier = int(tier_map.get(lg, 1)) if lg is not None else 1
        if tier == 2:
            factor *= 0.85
            reasons.append("liquidity_tier_2")
        elif tier == 3:
            factor *= 0.70
            reasons.append("liquidity_tier_3")

        # Combine
        final = max(0.0, min(1.0, base * factor))

        # E) Per-league cap (max_units)
        cap = cap_map.get(lg, None)
        if cap is not None:
            try:
                cap = float(cap)
                if final > cap:
                    final = cap
                    reasons.append(f"cap_{cap}")
            except Exception:
                pass

        # Optional global halt
        if risk_halt:
            final = 0.0
            reasons.append("RISK_HALT")

        rows.append({
            "fixture_id":  fid,
            "selection":   sel,
            "league":      lg,
            "base_kelly":  base,
            "stake_factor": round(factor, 3),
            "final_stake":  round(final, 3),
            "reasons":      ";".join(reasons) if reasons else ""
        })

    pd.DataFrame(rows).to_csv(OUT, index=False)
    print(f"kelly_policy_update: wrote {OUT} rows={len(rows)}")

if __name__ == "__main__":
    main()