#!/usr/bin/env python3
"""
forward_coverage_report.py — Are we predicting upcoming games?
Inputs:
  data/UPCOMING_fixtures.csv
  data/UPCOMING_7D_enriched.csv
  data/PRIORS_XG_SIM.csv
  data/PRIORS_AVAIL.csv
  data/PRIORS_SETPIECE.csv
  data/PRIORS_MKT.csv
  data/PRIORS_UNC.csv
Outputs:
  reports/FORWARD_COVERAGE.md

Reports:
- Fixtures count (7D)
- Per-league counts
- Priors completeness for next 7 days
- Missing fields that could block picks (availability, setpieces, odds, etc.)
"""

import os, pandas as pd, numpy as np
from datetime import datetime, timedelta

DATA="data"; REP="reports"
os.makedirs(REP, exist_ok=True)
FIX = os.path.join(DATA,"UPCOMING_fixtures.csv")
ENR = os.path.join(DATA,"UPCOMING_7D_enriched.csv")
OUT= os.path.join(REP,"FORWARD_COVERAGE.md")

PRI_FILES = [
    ("XG","PRIORS_XG_SIM.csv","fixture_id"),
    ("AV","PRIORS_AVAIL.csv","fixture_id"),
    ("SP","PRIORS_SETPIECE.csv","fixture_id"),
    ("MKT","PRIORS_MKT.csv","fixture_id"),
    ("UNC","PRIORS_UNC.csv","fixture_id"),
]

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
        df["fixture_id"]=df.apply(mk_id, axis=1)
    return df

def main():
    fx = ensure_fixture_id(safe_read(FIX))
    enr = ensure_fixture_id(safe_read(ENR))

    lines = ["# FORWARD COVERAGE", f"_Generated: {datetime.utcnow().isoformat()}Z_", ""]
    if fx.empty:
        lines += ["- No upcoming fixtures file; cannot assess coverage."]
        with open(OUT,"w",encoding="utf-8") as f: f.write("\n".join(lines)+"\n")
        print("forward_coverage_report: no fixtures"); return

    # count by league
    lines += [f"- Fixtures in next 7 days: **{len(fx)}**"]
    per_league = fx.groupby("league").size().reset_index(name="n").sort_values("n", ascending=False)
    if not per_league.empty:
        lines += ["", "## Fixtures by League", "", "| League | Count |", "|---|---:|"]
        for _, r in per_league.iterrows():
            lines.append(f"| {r['league']} | {int(r['n'])} |")

    # priors completeness for upcoming set
    pr_cov=[]
    for tag, fname, key in PRI_FILES:
        pri = safe_read(os.path.join(DATA,fname))
        if pri.empty or key not in pri.columns:
            pr_cov.append((tag, 0, len(fx)))
            continue
        got = set(pri[key].astype(str))
        have = set(ensure_fixture_id(fx)["fixture_id"].astype(str))
        n = len(have & got)
        pr_cov.append((tag, n, len(fx)))
    lines += ["", "## Priors completeness (next 7 days)", "", "| Prior | Covered | Total | % |", "|---|---:|---:|---:|"]
    for tag, n, total in pr_cov:
        pct = (n/total*100.0) if total>0 else 0.0
        lines.append(f"| {tag} | {n} | {total} | {pct:.0f}% |")

    # missing fields in enrichment (blocking signals)
    blocking = []
    need_cols = ["home_avail","away_avail","home_pass_pct","away_pass_pct","home_sca90","away_sca90",
                 "home_pressures90","away_pressures90","home_setpiece_share","away_setpiece_share",
                 "home_gk_psxg_prevented","away_gk_psxg_prevented","ou_main_total","bookmaker_count"]
    if enr.empty:
        blocking.append("Enrichment missing entirely.")
    else:
        miss = [c for c in need_cols if c not in enr.columns]
        if miss:
            blocking.append("Missing enrichment columns: " + ", ".join(miss))
    if blocking:
        lines += ["", "## Potential blockers", ""] + [f"- {b}" for b in blocking]
    else:
        lines += ["", "## Potential blockers", "", "- None detected."]

    with open(OUT,"w",encoding="utf-8") as f:
        f.write("\n".join(lines)+"\n")
    print(f"forward_coverage_report: wrote {OUT}")

if __name__ == "__main__":
    main()