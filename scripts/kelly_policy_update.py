#!/usr/bin/env python3
"""
kelly_policy_update.py — adjust stakes using market risk signals (FINAL)
Inputs:
  data/PREDICTIONS_7D.csv
  data/CONSISTENCY_CHECKS.csv
  data/UPCOMING_7D_enriched.csv
Outputs:
  data/ACTIONABILITY_REPORT.csv

Policy:
- Start with base_kelly in PREDICTIONS_7D.csv (if missing, assume 1.0 unit)
- Downweight if:
    A) Any contradiction flags (BTTS_vs_OU)
    B) High dispersion (>=12) with no opening/closing timing
    C) OU vs SPI divergence (new)
- final_stake = clip(base_kelly * stake_factor, 0..1)
"""

import os
import numpy as np
import pandas as pd

DATA = "data"
PRED = os.path.join(DATA, "PREDICTIONS_7D.csv")
CONS = os.path.join(DATA, "CONSISTENCY_CHECKS.csv")
ENR  = os.path.join(DATA, "UPCOMING_7D_enriched.csv")
OUT  = os.path.join(DATA, "ACTIONABILITY_REPORT.csv")

def safe_read(path):
    if not os.path.exists(path): return pd.DataFrame()
    try: return pd.read_csv(path)
    except Exception: return pd.DataFrame()

def mk_id(r):
    d = str(r.get("date","NA")).replace("-","").replace("T","_").replace(":","")
    h = str(r.get("home_team","NA")).strip().lower().replace(" ","_")
    a = str(r.get("away_team","NA")).strip().lower().replace(" ","_")
    return f"{d}__{h}__vs__{a}"

def main():
    pred = safe_read(PRED)
    cons = safe_read(CONS)
    enr  = safe_read(ENR)

    if pred.empty:
        pd.DataFrame(columns=["fixture_id","selection","base_kelly","stake_factor","final_stake","reasons"]).to_csv(OUT, index=False)
        print("kelly_policy_update: no predictions; wrote header-only")
        return

    if "fixture_id" not in pred.columns:
        pred["fixture_id"] = pred.apply(mk_id, axis=1)

    # lookups
    flag_sum = {}
    if not cons.empty and {"fixture_id","flag"}.issubset(cons.columns):
        flag_sum = cons.groupby("fixture_id")["flag"].sum().to_dict()

    disp_map, open_map, close_map = {}, {}, {}
    if not enr.empty and "fixture_id" in enr.columns:
        if "bookmaker_count"   in enr.columns: disp_map  = dict(zip(enr["fixture_id"], enr["bookmaker_count"]))
        if "has_opening_odds"  in enr.columns: open_map  = dict(zip(enr["fixture_id"], enr["has_opening_odds"]))
        if "has_closing_odds"  in enr.columns: close_map = dict(zip(enr["fixture_id"], enr["has_closing_odds"]))

    rows = []
    for _, r in pred.iterrows():
        fid = r["fixture_id"]
        sel = r.get("selection") or r.get("pick") or "N/A"
        base = r.get("kelly", np.nan)
        if pd.isna(base): base = 1.0

        reasons = []
        factor = 1.0

        # A) contradictions present
        if flag_sum.get(fid, 0) > 0:
            factor *= 0.6
            reasons.append("market_contradiction")

        # B) high dispersion without timing
        disp = disp_map.get(fid, np.nan)
        has_open = int(open_map.get(fid, 0)) if fid in open_map else 0
        has_close= int(close_map.get(fid, 0)) if fid in close_map else 0
        try:
            if pd.notna(disp) and float(disp) >= 12 and (not has_open) and (not has_close):
                factor *= 0.7
                reasons.append("high_dispersion_no_timing")
        except Exception:
            pass

        # C) OU vs SPI divergence flag
        if not cons.empty and {"fixture_id","check"}.issubset(cons.columns):
            sub = cons[(cons["fixture_id"]==fid) & (cons["check"]=="OU_vs_SPI")]
            if not sub.empty:
                factor *= 0.5
                reasons.append("ou_vs_spi_divergence")

        final = max(0.0, min(1.0, base * factor))
        rows.append({
            "fixture_id": fid,
            "selection": sel,
            "base_kelly": base,
            "stake_factor": round(factor, 3),
            "final_stake": round(final, 3),
            "reasons": ";".join(reasons) if reasons else ""
        })

    pd.DataFrame(rows).to_csv(OUT, index=False)
    print(f"kelly_policy_update: wrote {OUT} rows={len(rows)}")

if __name__ == "__main__":
    main()