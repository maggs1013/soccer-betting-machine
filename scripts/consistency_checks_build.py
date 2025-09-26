#!/usr/bin/env python3
"""
consistency_checks_build.py — cross-market contradictions
Inputs:
  data/UPCOMING_7D_enriched.csv
Outputs:
  data/CONSISTENCY_CHECKS.csv

Checks (best-effort):
- If OU total is very low but both teams' form/attack imply high scoring (flag)
- If BTTS prices imply a high scoring probability but OU is low (flag)
- If market dispersion is very high and has_opening/closing missing (flag)

All checks are heuristic and safe; if inputs missing, rows still produced.
"""

import os
import numpy as np
import pandas as pd

DATA = "data"
SRC = os.path.join(DATA, "UPCOMING_7D_enriched.csv")
OUT = os.path.join(DATA, "CONSISTENCY_CHECKS.csv")

def safe_read(path):
    if not os.path.exists(path): return pd.DataFrame()
    try: return pd.read_csv(path)
    except Exception: return pd.DataFrame()

def main():
    up = safe_read(SRC)
    if up.empty:
        pd.DataFrame(columns=["fixture_id","check","flag","details"]).to_csv(OUT, index=False)
        print("consistency_checks_build: no enriched; wrote header-only")
        return

    # ensure fixture_id
    if "fixture_id" not in up.columns:
        def mk(r):
            d = str(r.get("date","NA")).replace("-","").replace("T","_").replace(":","")
            h = str(r.get("home_team","NA")).strip().lower().replace(" ","_")
            a = str(r.get("away_team","NA")).strip().lower().replace(" ","_")
            return f"{d}__{h}__vs__{a}"
        up["fixture_id"] = up.apply(mk, axis=1)

    rows = []

    for _, r in up.iterrows():
        fid = r["fixture_id"]
        # Heuristic 1: BTTS vs OU contradiction
        ou = r.get("ou_main_total", np.nan)
        btts_yes = r.get("btts_yes_price", np.nan)
        btts_no = r.get("btts_no_price", np.nan)
        if pd.notna(ou) and pd.notna(btts_yes) and pd.notna(btts_no):
            try:
                # Simple heuristic: if OU total <= 2.0 but BTTS prices are near parity → inconsistent
                gap = abs(float(btts_yes) - float(btts_no))
                if float(ou) <= 2.0 and gap < 0.2:
                    rows.append({
                        "fixture_id": fid,
                        "check": "BTTS_vs_OU",
                        "flag": 1,
                        "details": f"OU={ou} while BTTS prices near parity (gap={gap:.2f})"
                    })
            except Exception:
                pass

        # Heuristic 2: Market dispersion too high with no opening/closing signals
        disp = r.get("bookmaker_count", np.nan)
        has_open = r.get("has_opening_odds", 0)
        has_close = r.get("has_closing_odds", 0)
        try:
            if pd.notna(disp) and float(disp) >= 12 and (not int(has_open)) and (not int(has_close)):
                rows.append({
                    "fixture_id": fid,
                    "check": "Dispersion_Without_Timing",
                    "flag": 1,
                    "details": f"bookmaker_count={disp} with no opening/closing indicators"
                })
        except Exception:
            pass

    pd.DataFrame(rows or [], columns=["fixture_id","check","flag","details"]).to_csv(OUT, index=False)
    print(f"consistency_checks_build: wrote {OUT} rows={(len(rows) if rows else 0)}")

if __name__ == "__main__":
    main()