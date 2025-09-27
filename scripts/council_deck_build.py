#!/usr/bin/env python3
"""
council_deck_build.py — One-glance, print-ready Council Deck (markdown)

Reads (safe with missing):
  data/ACTIONABILITY_REPORT.csv        # final stakes + reasons
  data/WHY_NOT_BET.csv                 # vetoed fixtures
  reports/WHY_NOT_BET.md               # grouped veto reasons
  reports/FEATURE_IMPORTANCE.md        # top features per league
  reports/FORWARD_COVERAGE.md          # upcoming coverage & priors completeness
  reports/POST_RUN_SANITY.md           # overall PASS/WARN/FAIL
  reports/DEEP_SANITY_PROBE.md         # file-by-file details (if present)
  data/UPCOMING_fixtures.csv           # context
  data/HISTORY_LOG.csv                 # (optional) last run timestamp
  data/UPCOMING_7D_model_matrix.csv    # (optional) numeric feature count
  data/UPCOMING_7D_enriched.csv        # (optional) enrichment row count

Writes:
  reports/COUNCIL_DECK.md
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime

DATA = "data"
REP  = "reports"
os.makedirs(REP, exist_ok=True)
OUT  = os.path.join(REP, "COUNCIL_DECK.md")

def safe_read_csv(p):
    if not os.path.exists(p): return pd.DataFrame()
    try: return pd.read_csv(p)
    except Exception: return pd.DataFrame()

def safe_read_md(p, max_lines=None):
    if not os.path.exists(p): return ""
    with open(p, "r", encoding="utf-8") as f:
        lines = f.readlines()
    if max_lines: lines = lines[:max_lines]
    return "".join(lines).strip()

def top_bets(actionability, top_n=25, min_stake=0.1):
    if actionability.empty: return []
    df = actionability.copy()
    if "final_stake" in df.columns:
        df = df[df["final_stake"]>min_stake]
        df = df.sort_values(["final_stake"], ascending=False).head(top_n)
    cols = [c for c in ["league","fixture_id","selection","final_stake","reasons"] if c in df.columns]
    if not cols: return []
    # pretty table
    lines = ["| League | Fixture | Selection | Stake | Reasons |", "|---|---|---|---:|---|"]
    for _, r in df[cols].iterrows():
        lines.append(f"| {r.get('league','')} | {r.get('fixture_id','')} | {r.get('selection','')} | {r.get('final_stake',0):.2f} | {r.get('reasons','')} |")
    return lines

def veto_summary(why_not_csv, why_not_md, max_rows=10):
    blocks = []
    # Grouped reasons MD (if present)
    if why_not_md:
        blocks += ["### Veto reasons (grouped)", "", why_not_md, ""]
    # Sample vetoes table from CSV
    if not why_not_csv.empty:
        samp = why_not_csv.head(max_rows)
        cols = [c for c in ["league","fixture_id","home_team","away_team","final_stake","reasons"] if c in samp.columns]
        if cols:
            lines = ["### Sample vetoed fixtures", "", "| League | Fixture | Home | Away | Stake | Reasons |", "|---|---|---|---|---:|---|"]
            for _, r in samp.iterrows():
                lines.append(f"| {r.get('league','')} | {r.get('fixture_id','')} | {r.get('home_team','')} | {r.get('away_team','')} | {r.get('final_stake','')} | {r.get('reasons','')} |")
            blocks += [""] + lines + [""]
    return blocks

def quick_stats():
    fx   = safe_read_csv(os.path.join(DATA,"UPCOMING_fixtures.csv"))
    enr  = safe_read_csv(os.path.join(DATA,"UPCOMING_7D_enriched.csv"))
    mtx  = safe_read_csv(os.path.join(DATA,"UPCOMING_7D_model_matrix.csv"))
    hist = safe_read_csv(os.path.join(DATA,"HISTORY_LOG.csv"))
    stats = []
    stats.append(f"- Fixtures (next 7d file rows): **{len(fx)}**")
    stats.append(f"- Enriched rows: **{len(enr)}**")
    if not mtx.empty:
        # numeric columns count (rough proxy of feature richness)
        num_cols = [c for c in mtx.columns if str(mtx[c].dtype).startswith(("float","int"))]
        stats.append(f"- Model matrix rows: **{len(mtx)}**, numeric features: **{len(num_cols)}**")
    if not hist.empty:
        try:
            last_run = hist["run_timestamp"].dropna().astype(str).iloc[-1]
            stats.append(f"- Last history log timestamp: **{last_run}**")
        except Exception:
            pass
    return stats

def main():
    # Read inputs
    actionability = safe_read_csv(os.path.join(DATA,"ACTIONABILITY_REPORT.csv"))
    why_not_csv   = safe_read_csv(os.path.join(DATA,"WHY_NOT_BET.csv"))

    why_not_md    = safe_read_md(os.path.join(REP,"WHY_NOT_BET.md"))
    feat_md       = safe_read_md(os.path.join(REP,"FEATURE_IMPORTANCE.md"), max_lines=400)  # keep it readable
    coverage_md   = safe_read_md(os.path.join(REP,"FORWARD_COVERAGE.md"))
    sanity_md     = safe_read_md(os.path.join(REP,"POST_RUN_SANITY.md"))
    deep_md       = safe_read_md(os.path.join(REP,"DEEP_SANITY_PROBE.md"))

    # Compose deck
    lines = []
    lines.append("# COUNCIL DECK")
    lines.append(f"_Generated: {datetime.utcnow().isoformat()}Z_")
    lines.append("")

    # 1) Health at a glance
    lines.append("## 1) Health at a glance")
    lines += [""] + quick_stats() + [""]
    if sanity_md:
        lines.append("### Sanity snapshot")
        lines.append("")
        lines.append(sanity_md)
        lines.append("")
    if coverage_md:
        lines.append("### Forward coverage snapshot")
        lines.append("")
        lines.append(coverage_md)
        lines.append("")

    # 2) Best bets
    lines.append("## 2) Best bets (top stakes)")
    tb = top_bets(actionability, top_n=25, min_stake=0.1)
    if tb:
        lines += [""] + tb + [""]
    else:
        lines += ["", "- (no positive-stake bets found in ACTIONABILITY_REPORT.csv)", ""]

    # 3) Veto transparency
    lines.append("## 3) Veto transparency")
    lines += veto_summary(why_not_csv, why_not_md, max_rows=10)

    # 4) What’s driving the model (feature importance)
    lines.append("## 4) Feature drivers (per league)")
    if feat_md:
        lines.append(feat_md)
        lines.append("")
    else:
        lines.append("- (no FEATURE_IMPORTANCE.md available)")
        lines.append("")

    # 5) Deep diagnostic (optional)
    lines.append("## 5) Deep diagnostic (file-by-file)")
    if deep_md:
        lines.append(deep_md)
        lines.append("")
    else:
        lines.append("- (no DEEP_SANITY_PROBE.md produced this run)")
        lines.append("")

    # write out
    with open(OUT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"council_deck_build: wrote {OUT}")

if __name__ == "__main__":
    main()