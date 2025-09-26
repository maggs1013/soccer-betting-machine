#!/usr/bin/env python3
"""
preflight_slo_append.py — append SLOs to AUTO_BRIEFING.md

SLOs included:
- fixtures count
- % FBref slices present (by consolidated per-slice CSVs)
- mean SPI CI width (volatility proxy)
- mean bookmaker_count (coverage proxy)
- contradictions count (CONSISTENCY_CHECKS)
"""

import os, glob, pandas as pd, numpy as np

DATA="data"; REP="reports"
AUTO=os.path.join(REP,"AUTO_BRIEFING.md")

def safe_read(p):
    if not os.path.exists(p): return pd.DataFrame()
    try: return pd.read_csv(p)
    except Exception: return pd.DataFrame()

def main():
    os.makedirs(REP, exist_ok=True)
    lines=[]
    fx = safe_read(os.path.join(DATA,"UPCOMING_fixtures.csv"))
    enr= safe_read(os.path.join(DATA,"UPCOMING_7D_enriched.csv"))
    spi= safe_read(os.path.join(DATA,"sd_538_spi.csv"))
    cons=safe_read(os.path.join(DATA,"CONSISTENCY_CHECKS.csv"))

    fixtures = len(fx)
    # FBref slice presence
    consolidated = glob.glob(os.path.join(DATA,"fbref_slice_*.csv"))
    # exclude per-season shards that have __ in name
    consolidated = [p for p in consolidated if "__" not in os.path.basename(p)]
    fbref_slices = len(consolidated)
    total_target = 10  # expected consolidated slices if you enable all
    pct_slices = (fbref_slices/total_target*100.0) if total_target>0 else 0

    mean_ci = np.nan
    if not spi.empty and "spi_ci_width" in spi.columns:
        mean_ci = pd.to_numeric(spi["spi_ci_width"], errors="coerce").mean()

    mean_books = np.nan
    if not enr.empty and "bookmaker_count" in enr.columns:
        mean_books = pd.to_numeric(enr["bookmaker_count"], errors="coerce").mean()

    contradictions = 0
    if not cons.empty: 
        contradictions = len(cons)

    lines.append("\n---\n## Preflight SLOs\n")
    lines.append(f"- Fixtures fetched: **{fixtures}**")
    lines.append(f"- FBref slices present (consolidated): **{fbref_slices}** (~{pct_slices:.0f}% of target)")
    lines.append(f"- Mean SPI CI width (volatility): **{mean_ci:.3f}**" if not np.isnan(mean_ci) else "- Mean SPI CI width: N/A")
    lines.append(f"- Mean bookmaker_count (coverage): **{mean_books:.2f}**" if not np.isnan(mean_books) else "- Mean bookmaker_count: N/A")
    lines.append(f"- Market contradictions flagged: **{contradictions}**")

    # Append to AUTO_BRIEFING.md
    prev=""
    if os.path.exists(AUTO):
        with open(AUTO,"r",encoding="utf-8") as f: prev=f.read()
    with open(AUTO,"w",encoding="utf-8") as f:
        f.write(prev + "\n" + "\n".join(lines) + "\n")
    print("preflight_slo_append: appended SLOs to briefing")

if __name__ == "__main__":
    main()