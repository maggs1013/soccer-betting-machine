#!/usr/bin/env python3
"""
consistency_checks_build.py — cross-market contradictions (FINAL)
Inputs:  data/UPCOMING_7D_enriched.csv
Outputs: data/CONSISTENCY_CHECKS.csv

Checks (best-effort):
- BTTS vs OU contradiction (OU low but BTTS near parity)
- High dispersion but no opening/closing timestamps
- NEW: OU vs SPI expected-goals proxy divergence
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

def mk_id(r):
    d = str(r.get("date","NA")).replace("-","").replace("T","_").replace(":","")
    h = str(r.get("home_team","NA")).strip().lower().replace(" ","_")
    a = str(r.get("away_team","NA")).strip().lower().replace(" ","_")
    return f"{d}__{h}__vs__{a}"

def main():
    up = safe_read(SRC)
    if up.empty:
        pd.DataFrame(columns=["fixture_id","check","flag","details"]).to_csv(OUT, index=False)
        print("consistency_checks_build: no enriched; wrote header-only")
        return

    if "fixture_id" not in up.columns:
        up["fixture_id"] = up.apply(mk_id, axis=1)

    rows = []

    for _, r in up.iterrows():
        fid = r["fixture_id"]

        # 1) BTTS vs OU contradiction
        ou = r.get("ou_main_total", np.nan)
        btts_yes = r.get("btts_yes_price", np.nan)
        btts_no  = r.get("btts_no_price",  np.nan)
        if pd.notna(ou) and pd.notna(btts_yes) and pd.notna(btts_no):
            try:
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

        # 2) Dispersion without timing clues
        disp = r.get("bookmaker_count", np.nan)
        has_open = int(r.get("has_opening_odds", 0)) if "has_opening_odds" in up.columns else 0
        has_close= int(r.get("has_closing_odds", 0)) if "has_closing_odds" in up.columns else 0
        try:
            if pd.notna(disp) and float(disp) >= 12 and (not has_open) and (not has_close):
                rows.append({
                    "fixture_id": fid,
                    "check": "Dispersion_Without_Timing",
                    "flag": 1,
                    "details": f"bookmaker_count={disp} with no opening/closing indicators"
                })
        except Exception:
            pass

        # 3) OU vs SPI expected-goals proxy
        # simple proxy: if ranks are close (|diff|<20) expect ~3.0 goals, else ~2.2
        spi_h = r.get("home_spi_rank", np.nan)
        spi_a = r.get("away_spi_rank", np.nan)
        if pd.notna(ou) and pd.notna(spi_h) and pd.notna(spi_a):
            try:
                diff = abs(float(spi_h) - float(spi_a))
                exp_goals = 3.0 if diff < 20 else 2.2
                if abs(float(ou) - exp_goals) > 1.0:
                    rows.append({
                        "fixture_id": fid,
                        "check": "OU_vs_SPI",
                        "flag": 1,
                        "details": f"OU={ou} vs SPI proxy={exp_goals:.1f} (rank_diff={diff:.1f})"
                    })
            except Exception:
                pass

    pd.DataFrame(rows or [], columns=["fixture_id","check","flag","details"]).to_csv(OUT, index=False)
    print(f"consistency_checks_build: wrote {OUT} rows={(len(rows) if rows else 0)}")

if __name__ == "__main__":
    main()