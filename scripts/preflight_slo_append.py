#!/usr/bin/env python3
"""
preflight_slo_append.py — append SLOs to AUTO_BRIEFING.md

SLOs included:
- fixtures count
- FBref consolidated slice presence count (proxy for slice coverage)
- mean SPI CI width (volatility proxy)
- mean bookmaker_count (coverage proxy)
- contradictions count (CONSISTENCY_CHECKS)
- Priors completeness: % fixtures present in ALL five PRIORS_* files
"""

import os, glob
import pandas as pd
import numpy as np

DATA = "data"
REP  = "reports"
AUTO = os.path.join(REP, "AUTO_BRIEFING.md")
os.makedirs(REP, exist_ok=True)

def safe_read(p):
    if not os.path.exists(p): return pd.DataFrame()
    try: return pd.read_csv(p)
    except Exception: return pd.DataFrame()

def ensure_auto():
    if not os.path.exists(AUTO):
        with open(AUTO, "w", encoding="utf-8") as f:
            f.write("# AUTO BRIEFING\n")

def priors_completeness():
    """Return (coverage_pct, n_fixtures, n_full) for fixtures that exist in all five priors."""
    fx = safe_read(os.path.join(DATA, "UPCOMING_fixtures.csv"))
    if fx.empty:
        return (0.0, 0, 0)

    # Ensure fixture_id
    if "fixture_id" not in fx.columns:
        def mk_id(r):
            d = str(r.get("date","NA")).replace("-", "").replace("T", "_").replace(":", "")
            h = str(r.get("home_team","NA")).strip().lower().replace(" ", "_")
            a = str(r.get("away_team","NA")).strip().lower().replace(" ", "_")
            return f"{d}__{h}__vs__{a}"
        fx["fixture_id"] = fx.apply(mk_id, axis=1)

    pri_files = [
        "PRIORS_XG_SIM.csv",
        "PRIORS_AVAIL.csv",
        "PRIORS_SETPIECE.csv",
        "PRIORS_MKT.csv",
        "PRIORS_UNC.csv",
    ]
    pri_sets = []
    for rel in pri_files:
        df = safe_read(os.path.join(DATA, rel))
        if df.empty or "fixture_id" not in df.columns:
            pri_sets.append(set())  # empty → no coverage
        else:
            pri_sets.append(set(df["fixture_id"].dropna().astype(str)))

    all_fids = set(fx["fixture_id"].dropna().astype(str))
    full_covered = set.intersection(*pri_sets) if pri_sets else set()
    n_fixtures = len(all_fids)
    n_full = len(all_fids & full_covered)
    pct = (n_full / n_fixtures * 100.0) if n_fixtures > 0 else 0.0
    return (pct, n_fixtures, n_full)

def main():
    ensure_auto()
    lines = []

    fx   = safe_read(os.path.join(DATA, "UPCOMING_fixtures.csv"))
    enr  = safe_read(os.path.join(DATA, "UPCOMING_7D_enriched.csv"))
    spi  = safe_read(os.path.join(DATA, "sd_538_spi.csv"))
    cons = safe_read(os.path.join(DATA, "CONSISTENCY_CHECKS.csv"))

    fixtures = len(fx)

    # FBref slice coverage proxy: count consolidated slice CSVs (exclude per-season shards)
    consolidated = glob.glob(os.path.join(DATA, "fbref_slice_*.csv"))
    consolidated = [p for p in consolidated if "__" not in os.path.basename(p)]
    fbref_slices = len(consolidated)
    total_target = 10  # adjust if you routinely enable more/less
    pct_slices = (fbref_slices / total_target * 100.0) if total_target > 0 else 0

    mean_ci = np.nan
    if not spi.empty and "spi_ci_width" in spi.columns:
        mean_ci = pd.to_numeric(spi["spi_ci_width"], errors="coerce").mean()

    mean_books = np.nan
    if not enr.empty and "bookmaker_count" in enr.columns:
        mean_books = pd.to_numeric(enr["bookmaker_count"], errors="coerce").mean()

    contradictions = len(cons) if not cons.empty else 0

    pri_pct, pri_n, pri_full = priors_completeness()

    # Compose SLO block
    lines.append("\n---\n## Preflight SLOs\n")
    lines.append(f"- Fixtures fetched: **{fixtures}**")
    lines.append(f"- FBref slices present (consolidated): **{fbref_slices}** (~{pct_slices:.0f}% of target)")
    lines.append(f"- Mean SPI CI width (volatility): **{mean_ci:.3f}**" if not np.isnan(mean_ci) else "- Mean SPI CI width: N/A")
    lines.append(f"- Mean bookmaker_count (coverage): **{mean_books:.2f}**" if not np.isnan(mean_books) else "- Mean bookmaker_count: N/A")
    lines.append(f"- Market contradictions flagged: **{contradictions}**")
    lines.append(f"- Priors completeness: **{pri_pct:.0f}%** ({pri_full}/{pri_n} fixtures with **all five** priors)")

    # Append to briefing
    with open(AUTO, "a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print("preflight_slo_append: appended SLOs to briefing")

if __name__ == "__main__":
    main()