#!/usr/bin/env python3
"""
forward_coverage_report.py — Are we predicting upcoming games in the next 7 days?

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

What it reports:
- Count of fixtures strictly in the next 7 days (UTC)
- Fixtures by league (counts)
- Priors completeness for those fixtures (overall + per league)
- Enrichment readiness: missing columns & % missing by required field (blocking risk)
- Odds readiness: presence of key odds fields and line-move fields (if connector supports them)
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta

DATA = "data"
REP  = "reports"
os.makedirs(REP, exist_ok=True)

FIX = os.path.join(DATA, "UPCOMING_fixtures.csv")
ENR = os.path.join(DATA, "UPCOMING_7D_enriched.csv")
OUT = os.path.join(REP,  "FORWARD_COVERAGE.md")

PRI_FILES = [
    ("XG",  os.path.join(DATA, "PRIORS_XG_SIM.csv"),  "fixture_id"),
    ("AV",  os.path.join(DATA, "PRIORS_AVAIL.csv"),   "fixture_id"),
    ("SP",  os.path.join(DATA, "PRIORS_SETPIECE.csv"),"fixture_id"),
    ("MKT", os.path.join(DATA, "PRIORS_MKT.csv"),     "fixture_id"),
    ("UNC", os.path.join(DATA, "PRIORS_UNC.csv"),     "fixture_id"),
]

REQUIRED_ENR = [
    "home_avail","away_avail",
    "home_pass_pct","away_pass_pct",
    "home_sca90","away_sca90",
    "home_pressures90","away_pressures90",
    "home_setpiece_share","away_setpiece_share",
    "home_gk_psxg_prevented","away_gk_psxg_prevented",
    "ou_main_total","bookmaker_count"
]

LINE_MOVE_CANDIDATES = [
    "ou_move",
    "h2h_home_move","h2h_draw_move","h2h_away_move",
    "spread_home_line_move","spread_away_line_move"
]

def safe_read(p):
    if not os.path.exists(p): return pd.DataFrame()
    try: return pd.read_csv(p)
    except Exception: return pd.DataFrame()

def ensure_fixture_id(df):
    if df.empty: return df
    if "fixture_id" in df.columns: return df
    if {"date","home_team","away_team"}.issubset(df.columns):
        def mk_id(r):
            d = str(r.get("date","NA")).replace("-","").replace("T","_").replace(":","")
            h = str(r.get("home_team","NA")).strip().lower().replace(" ","_")
            a = str(r.get("away_team","NA")).strip().lower().replace(" ","_")
            return f"{d}__{h}__vs__{a}"
        df = df.copy()
        df["fixture_id"] = df.apply(mk_id, axis=1)
    return df

def parse_utc(series):
    """Parse ISO-ish strings to UTC-aware pandas datetimes (coerce errors)."""
    dt = pd.to_datetime(series, errors="coerce", utc=True)
    # If naive (no tz), assume UTC
    if getattr(dt.dtype, "tz", None) is None:
        dt = pd.to_datetime(series, errors="coerce").dt.tz_localize(timezone.utc, nonexistent="NaT", ambiguous="NaT")
    return dt

def main():
    now_utc = datetime.now(timezone.utc)
    end_utc = now_utc + timedelta(days=7)

    fx  = ensure_fixture_id(safe_read(FIX))
    enr = ensure_fixture_id(safe_read(ENR))

    lines = ["# FORWARD COVERAGE", f"_Generated: {now_utc.isoformat()}_", ""]

    if fx.empty:
        lines += ["- No `UPCOMING_fixtures.csv`; cannot assess forward coverage."]
        with open(OUT, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
        print("forward_coverage_report: no fixtures file; wrote report with note.")
        return

    # Filter fixtures strictly in next 7 days ( [now, now+7d] )
    if "date" in fx.columns:
        fx["_dt"] = parse_utc(fx["date"])
        mask = fx["_dt"].notna() & (fx["_dt"] >= now_utc) & (fx["_dt"] <= end_utc)
        fx7 = fx.loc[mask].copy()
    else:
        fx7 = fx.copy()

    lines += [f"- Fixtures in next 7 days: **{len(fx7)}**"]
    if len(fx7)==0:
        with open(OUT, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
        print("forward_coverage_report: zero fixtures in next 7 days.")
        return

    # Fixtures by league
    by_league = fx7.groupby("league").size().reset_index(name="n").sort_values("n", ascending=False)
    lines += ["", "## Fixtures by League (next 7 days)", "", "| League | Count |", "|---|---:|"]
    for _, r in by_league.iterrows():
        lines.append(f"| {r['league']} | {int(r['n'])} |")

    # Priors completeness (overall & per-league)
    have_ids = set(fx7["fixture_id"].astype(str))
    pri_sets = {}
    for tag, pth, key in PRI_FILES:
        pri = safe_read(pth)
        pri_sets[tag] = set(pri[key].astype(str)) if (not pri.empty and key in pri.columns) else set()

    def coverage_for(ids):
        cov = {}
        for tag, s in pri_sets.items():
            cov[tag] = len(ids & s)
        cov["ALL5"] = sum(1 for fid in ids if all(fid in pri_sets[tag] for tag in ["XG","AV","SP","MKT","UNC"]))
        return cov

    cov_all = coverage_for(have_ids)
    lines += ["", "## Priors completeness (overall, next 7 days)", "",
              "| Prior | Covered | Total | % |", "|---|---:|---:|---:|"]
    total = len(have_ids)
    for tag in ["XG","AV","SP","MKT","UNC","ALL5"]:
        n = cov_all.get(tag, 0)
        pct = (n/total*100.0) if total>0 else 0.0
        label = tag if tag!="ALL5" else "All five priors"
        lines.append(f"| {label} | {n} | {total} | {pct:.0f}% |")

    # Per-league priors completeness (ALL5)
    lines += ["", "### Priors completeness by league (ALL five priors)"]
    lines += ["", "| League | Covered | Total | % |", "|---|---:|---:|---:|"]
    for _, r in by_league.iterrows():
        lg = r["league"]
        ids_lg = set(fx7.loc[fx7["league"]==lg, "fixture_id"].astype(str))
        cov_lg = coverage_for(ids_lg)
        n = cov_lg["ALL5"]; tot = len(ids_lg); pct = (n/tot*100.0) if tot>0 else 0.0
        lines.append(f"| {lg} | {n} | {tot} | {pct:.0f}% |")

    # Enrichment readiness: required columns present & % missing rows per field
    lines += ["", "## Enrichment readiness (required fields)"]
    if enr.empty:
        lines += ["- Enriched table missing entirely."]
    else:
        # restrict enrichment to same fixture set when possible
        if "fixture_id" in enr.columns:
            enr7 = enr[enr["fixture_id"].astype(str).isin(have_ids)].copy()
        else:
            enr7 = enr.copy()

        miss_cols = [c for c in REQUIRED_ENR if c not in enr7.columns]
        if miss_cols:
            lines += ["- Missing columns: " + ", ".join(miss_cols)]
        # % missing by field (only for fields that exist)
        if len(enr7):
            lines += ["", "| Field | % Missing |", "|---|---:|"]
            for c in REQUIRED_ENR:
                if c in enr7.columns:
                    pct_miss = (enr7[c].isna().mean()*100.0)
                    lines.append(f"| {c} | {pct_miss:.0f}% |")

    # Odds readiness & line-moves presence
    lines += ["", "## Odds readiness"]
    if enr.empty:
        lines += ["- No enrichment; cannot assess odds readiness."]
    else:
        basic_odds = []
        for c in ["home_odds_dec","draw_odds_dec","away_odds_dec","ou_main_total","bookmaker_count"]:
            if c in enr.columns:
                basic_odds.append(c)
        if basic_odds:
            lines += ["- Basic odds fields present: " + ", ".join(basic_odds)]
        else:
            lines += ["- Basic odds fields missing."]

        lm_present = [c for c in LINE_MOVE_CANDIDATES if c in enr.columns]
        if lm_present:
            # also check if any non-null values exist
            any_nonnull = any(enr[c].notna().any() for c in lm_present)
            lines += [f"- Line-move fields present: {', '.join(lm_present)}" + (" (non-null values detected)" if any_nonnull else " (all null this run)")]
        else:
            lines += ["- No line-move fields found (connector may not persist opening/closing snapshots)."]

    with open(OUT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"forward_coverage_report: wrote {OUT}")

if __name__ == "__main__":
    main()