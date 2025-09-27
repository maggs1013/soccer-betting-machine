#!/usr/bin/env python3
"""
post_run_sanity.py — one-page PASS/WARN/FAIL health summary

Inputs (best-effort; safe with missing):
  data/UPCOMING_fixtures.csv
  data/UPCOMING_7D_enriched.csv
  data/UPCOMING_7D_model_matrix.csv
  data/ACTIONABILITY_REPORT.csv
  data/CONSISTENCY_CHECKS.csv
  data/PRIORS_XG_SIM.csv
  data/PRIORS_AVAIL.csv
  data/PRIORS_SETPIECE.csv
  data/PRIORS_MKT.csv
  data/PRIORS_UNC.csv
  reports/BLANK_FILE_ALERTS.md
  reports/AUTO_BRIEFING.md
  reports/CALIBRATION_REPORT.csv
  reports/TRAINING_REPORT.md
  reports/STACK_TRAINING_REPORT.md
  reports/FEATURE_IMPORTANCE.md

Output:
  reports/POST_RUN_SANITY.md

Scoring (heuristics):
- PASS ✅  when healthy
- WARN ⚠️  when degraded but usable
- FAIL ❌  when critical artifacts are blank or SLOs unacceptable
"""

import os, re
import numpy as np
import pandas as pd
from datetime import datetime

DATA = "data"
REP  = "reports"
os.makedirs(REP, exist_ok=True)
OUT  = os.path.join(REP, "POST_RUN_SANITY.md")

def safe_read_csv(p):
    if not os.path.exists(p): return pd.DataFrame()
    try: return pd.read_csv(p)
    except Exception: return pd.DataFrame()

def rows(p):
    if not os.path.exists(p): return -1
    try:
        return len(pd.read_csv(p))
    except Exception:
        return -2

def parse_blank_alerts(md_path):
    if not os.path.exists(md_path): return (False, [])
    crit = False
    issues = []
    with open(md_path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if line.startswith("- ❌"):
                crit = True
                issues.append(line)
            elif line.startswith("- ⚠️") or line.startswith("- ℹ️"):
                issues.append(line)
    return (crit, issues)

def parse_slos(auto_md):
    """Return dict with SLOs: fixtures, fbref_slices, mean_ci, mean_books, contradictions, priors_completeness."""
    slo = dict(fixtures=None, fbref_slices=None, mean_ci=None, mean_books=None,
               contradictions=None, priors_completeness=None)
    if not os.path.exists(auto_md): return slo
    with open(auto_md, "r", encoding="utf-8") as f:
        text = f.read()
    # naive regex extraction
    m = re.search(r"Fixtures fetched:\s*\*\*(\d+)\*\*", text)
    if m: slo["fixtures"] = int(m.group(1))
    m = re.search(r"FBref slices present.*\*\*(\d+)\*\*", text)
    if m: slo["fbref_slices"] = int(m.group(1))
    m = re.search(r"Mean SPI CI width.*\*\*([0-9\.\-NaN]+)\*\*", text)
    if m:
        try: slo["mean_ci"] = float(m.group(1))
        except: slo["mean_ci"] = None
    m = re.search(r"Mean bookmaker_count.*\*\*([0-9\.\-NaN]+)\*\*", text)
    if m:
        try: slo["mean_books"] = float(m.group(1))
        except: slo["mean_books"] = None
    m = re.search(r"Market contradictions flagged:\s*\*\*(\d+)\*\*", text)
    if m: slo["contradictions"] = int(m.group(1))
    m = re.search(r"Priors completeness:\s*\*\*(\d+)%\*\*\s*\((\d+)/(\d+)\s*fixtures", text)
    if m:
        slo["priors_completeness"] = dict(pct=int(m.group(1)), have=int(m.group(2)), total=int(m.group(3)))
    return slo

def status_emoji(level):  # "PASS" | "WARN" | "FAIL"
    return {"PASS":"✅","WARN":"⚠️","FAIL":"❌"}[level]

def main():
    lines = ["# POST-RUN SANITY", f"_Generated: {datetime.utcnow().isoformat()}Z_", ""]

    # 1) Critical artifacts
    crit_files = [
        "UPCOMING_fixtures.csv",
        "UPCOMING_7D_enriched.csv",
        "UPCOMING_7D_model_matrix.csv",
        "ACTIONABILITY_REPORT.csv",
    ]
    crit_status = "PASS"; crit_notes=[]
    for rel in crit_files:
        n = rows(os.path.join(DATA, rel))
        if n <= 0:
            crit_status = "FAIL"
            reason = "missing" if n==-1 else ("unreadable" if n==-2 else "blank")
            crit_notes.append(f"{rel} → {reason}")
        elif n < 5:
            if crit_status != "FAIL":
                crit_status = "WARN"
            crit_notes.append(f"{rel} → low rows ({n})")
    lines += [f"## Critical Artifacts: {status_emoji(crit_status)} {crit_status}"]
    lines += [f"- {s}" for s in crit_notes] or ["- All critical artifacts present with healthy row counts."]
    lines.append("")

    # 2) Blank file guard
    blank_fail, blank_issues = parse_blank_alerts(os.path.join(REP,"BLANK_FILE_ALERTS.md"))
    blank_status = "FAIL" if blank_fail else ("WARN" if blank_issues else "PASS")
    lines += [f"## Blank File Guard: {status_emoji(blank_status)} {blank_status}"]
    if blank_issues:
        lines += blank_issues
    else:
        lines += ["- No critical/blank issues detected by guard."]
    lines.append("")

    # 3) SLOs
    slo = parse_slos(os.path.join(REP,"AUTO_BRIEFING.md"))
    slo_status = "PASS"; slo_notes=[]
    # Priors completeness thresholds
    pc = slo.get("priors_completeness")
    if pc:
        pct = pc["pct"]
        if pct < 40: slo_status="FAIL"
        elif pct < 70 and slo_status!="FAIL": slo_status="WARN"
        slo_notes.append(f"Priors completeness: {pct}% ({pc['have']}/{pc['total']})")
    else:
        slo_status = "WARN"; slo_notes.append("Priors completeness: unknown")
    # Mean bookmaker_count low?
    mb = slo.get("mean_books")
    if mb is not None:
        if mb < 3 and slo_status!="FAIL": slo_status="WARN"; slo_notes.append(f"Low market coverage (mean books ~ {mb:.2f})")
    # Contradictions heuristic: > fixtures * 0.5 warns
    fx_n = slo.get("fixtures") or 0
    contradictions = slo.get("contradictions") or 0
    if fx_n>0 and contradictions > 0.5*fx_n and slo_status!="FAIL":
        slo_status = "WARN"; slo_notes.append(f"High contradictions: {contradictions}/{fx_n}")
    lines += [f"## SLOs: {status_emoji(slo_status)} {slo_status}"]
    lines += [f"- {s}" for s in slo_notes]
    lines.append("")

    # 4) Priors presence (non-blank rows)
    pri_files = ["PRIORS_XG_SIM.csv","PRIORS_AVAIL.csv","PRIORS_SETPIECE.csv","PRIORS_MKT.csv","PRIORS_UNC.csv"]
    pri_status="PASS"; pri_notes=[]
    for rel in pri_files:
        n = rows(os.path.join(DATA, rel))
        if n == 0:
            if pri_status!="FAIL": pri_status="WARN"
            pri_notes.append(f"{rel}: blank (0 rows)")
        elif n in (-1,-2):
            pri_status="WARN"
            pri_notes.append(f"{rel}: missing/unreadable (rows={n})")
    lines += [f"## Priors Layer: {status_emoji(pri_status)} {pri_status}"]
    lines += [f"- {s}" for s in pri_notes] or ["- All priors present."]
    lines.append("")

    # 5) Features & Matrix basic check
    feat_status="PASS"; feat_notes=[]
    if rows(os.path.join(DATA,"UPCOMING_7D_features.csv")) <= 0:
        feat_status="WARN"; feat_notes.append("UPCOMING_7D_features.csv is blank/missing")
    lines += [f"## Features & Matrix: {status_emoji(feat_status)} {feat_status}"]
    lines += [f"- {s}" for s in feat_notes] or ["- Features and model matrix present."]
    lines.append("")

    # 6) Calibration & Training presence
    cal_status="PASS"; cal_notes=[]
    if rows(os.path.join(REP,"CALIBRATION_REPORT.csv")) <= 0:
        cal_status="WARN"; cal_notes.append("CALIBRATION_REPORT.csv empty or missing")
    if not os.path.exists(os.path.join(REP,"TRAINING_REPORT.md")):
        cal_status = "WARN" if cal_status!="FAIL" else "FAIL"
        cal_notes.append("TRAINING_REPORT.md missing (minimal trainer skipped)")
    if not os.path.exists(os.path.join(REP,"STACK_TRAINING_REPORT.md")):
        cal_status = "WARN" if cal_status!="FAIL" else "FAIL"
        cal_notes.append("STACK_TRAINING_REPORT.md missing (stack trainer skipped)")
    if not os.path.exists(os.path.join(REP,"FEATURE_IMPORTANCE.md")):
        cal_status = "WARN" if cal_status!="FAIL" else "FAIL"
        cal_notes.append("FEATURE_IMPORTANCE.md missing")
    lines += [f"## Calibration & Training: {status_emoji(cal_status)} {cal_status}"]
    lines += [f"- {s}" for s in cal_notes] or ["- Calibration & trainers present; importance report generated."]
    lines.append("")

    # 7) Risk & Execution signal
    risk_status="PASS"; risk_notes=[]
    if rows(os.path.join(DATA,"ACTIONABILITY_REPORT.csv")) <= 0:
        risk_status="FAIL"; risk_notes.append("ACTIONABILITY_REPORT.csv blank/missing (cannot stake)")
    if rows(os.path.join(DATA,"WHY_NOT_BET.csv")) < 0:
        risk_status = "WARN" if risk_status!="FAIL" else "FAIL"
        risk_notes.append("WHY_NOT_BET.csv missing")
    lines += [f"## Risk & Execution: {status_emoji(risk_status)} {risk_status}"]
    lines += [f"- {s}" for s in risk_notes] or ["- Actionability & why-not-bet reports present."]
    lines.append("")

    # Overall verdict
    # If any FAIL in critical or blank guard: FAIL; else if any WARN anywhere: WARN; else PASS
    overall="PASS"
    if crit_status=="FAIL" or blank_status=="FAIL":
        overall="FAIL"
    else:
        for status in [slo_status, pri_status, feat_status, cal_status, risk_status]:
            if status=="WARN":
                overall="WARN"
    lines = [f"# OVERALL: {status_emoji(overall)} {overall}", ""] + lines

    with open(OUT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"post_run_sanity: wrote {OUT}")

if __name__ == "__main__":
    main()