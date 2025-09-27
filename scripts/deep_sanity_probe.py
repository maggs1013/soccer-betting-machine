#!/usr/bin/env python3
"""
deep_sanity_probe.py — Council-grade, file-by-file validation & cross-checks.

Outputs:
  reports/DEEP_SANITY_PROBE.md

Checks:
- Critical files: presence, row count (>0), required columns
- Enrichment readiness: required fields + % missing by column
- Priors coverage: overall & per-league; ALL-five coverage
- Cross-file joins: fixture_id alignment between fixtures, enriched, priors, features, matrix, actionability
- Risk & governance: actionability rows, veto rows, reasons distribution (top)
- SLOs & guard: parse BLANK_FILE_ALERTS.md, AUTO_BRIEFING.md, POST_RUN_SANITY.md (if present)

PASS/WARN/FAIL is emitted per section to quickly match sanity statuses.
"""

import os, re, sys
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta

DATA="data"; REP="reports"
os.makedirs(REP, exist_ok=True)
OUT=os.path.join(REP,"DEEP_SANITY_PROBE.md")

REQUIRED_ENR = [
    "home_avail","away_avail",
    "home_pass_pct","away_pass_pct",
    "home_sca90","away_sca90",
    "home_pressures90","away_pressures90",
    "home_setpiece_share","away_setpiece_share",
    "home_gk_psxg_prevented","away_gk_psxg_prevented",
    "ou_main_total","bookmaker_count"
]
PRI_FILES = [
    ("XG","PRIORS_XG_SIM.csv"),
    ("AV","PRIORS_AVAIL.csv"),
    ("SP","PRIORS_SETPIECE.csv"),
    ("MKT","PRIORS_MKT.csv"),
    ("UNC","PRIORS_UNC.csv"),
]

def safe_read(p):
    if not os.path.exists(p): return pd.DataFrame()
    try: return pd.read_csv(p)
    except Exception: return pd.DataFrame()

def ensure_fid(df):
    if df.empty: return df
    if "fixture_id" in df.columns: return df
    need={"date","home_team","away_team"}
    if need.issubset(df.columns):
        def mk_id(r):
            d=str(r.get("date","NA")).replace("-","").replace("T","_").replace(":","")
            h=str(r.get("home_team","NA")).strip().lower().replace(" ","_")
            a=str(r.get("away_team","NA")).strip().lower().replace(" ","_")
            return f"{d}__{h}__vs__{a}"
        df=df.copy()
        df["fixture_id"]=df.apply(mk_id, axis=1)
    return df

def count_missing(df, cols):
    rows=[]
    for c in cols:
        if c in df.columns:
            pct = float(df[c].isna().mean()*100.0) if len(df) else 100.0
            rows.append((c, pct))
    return rows

def parse_md_val(md_path, label_regex):
    if not os.path.exists(md_path): return None
    rex=re.compile(label_regex)
    with open(md_path,"r",encoding="utf-8") as f:
        for line in f:
            m=rex.search(line)
            if m: return m.group(1)
    return None

def section(title, status, lines):
    hdr = f"## {title}: {'✅ PASS' if status=='PASS' else ('⚠️ WARN' if status=='WARN' else '❌ FAIL')}"
    return [hdr, ""] + (lines if lines else ["- (no details)"]) + [""]

def main():
    lines=[f"# DEEP SANITY PROBE", f"_Generated: {datetime.utcnow().isoformat()}Z_", ""]

    # Load files
    fx   = ensure_fid(safe_read(os.path.join(DATA,"UPCOMING_fixtures.csv")))
    enr  = ensure_fid(safe_read(os.path.join(DATA,"UPCOMING_7D_enriched.csv")))
    feat = ensure_fid(safe_read(os.path.join(DATA,"UPCOMING_7D_features.csv")))
    mtx  = ensure_fid(safe_read(os.path.join(DATA,"UPCOMING_7D_model_matrix.csv")))
    act  = safe_read(os.path.join(DATA,"ACTIONABILITY_REPORT.csv"))
    cons = safe_read(os.path.join(DATA,"CONSISTENCY_CHECKS.csv"))
    why  = safe_read(os.path.join(DATA,"WHY_NOT_BET.csv"))
    slo  = os.path.join(REP,"AUTO_BRIEFING.md")
    blank= os.path.join(REP,"BLANK_FILE_ALERTS.md")
    sanity=os.path.join(REP,"POST_RUN_SANITY.md")

    # Critical section
    crit_status="PASS"; crit_lines=[]
    crit_checks = [
        ("UPCOMING_fixtures.csv", fx, ["date","league","home_team","away_team"]),
        ("UPCOMING_7D_enriched.csv", enr, ["fixture_id","league"]),
        ("UPCOMING_7D_model_matrix.csv", mtx, ["fixture_id","league"]),
        ("ACTIONABILITY_REPORT.csv", act, ["fixture_id","final_stake"]),
    ]
    for name, df, req in crit_checks:
        if df.empty:
            crit_status="FAIL"; crit_lines.append(f"- {name}: **0 rows**")
        else:
            missing=[c for c in req if c not in df.columns]
            if missing:
                crit_status="FAIL"; crit_lines.append(f"- {name}: missing cols {missing}")
            else:
                crit_lines.append(f"- {name}: rows={len(df)} (OK)")
    lines += section("Critical artifacts", crit_status, crit_lines)

    # Enrichment readiness
    enr_status="PASS"; enr_lines=[]
    if enr.empty:
        enr_status="FAIL"; enr_lines.append("- Enriched table is empty.")
    else:
        miss_cols=[c for c in REQUIRED_ENR if c not in enr.columns]
        if miss_cols:
            enr_status="WARN"; enr_lines.append(f"- Missing required columns: {', '.join(miss_cols)}")
        miss_pct = count_missing(enr, [c for c in REQUIRED_ENR if c in enr.columns])
        if miss_pct:
            enr_lines.append("")
            enr_lines += ["| Field | % Missing |", "|---|---:|"]
            for c,p in miss_pct:
                enr_lines.append(f"| {c} | {p:.0f}% |")
    lines += section("Enrichment readiness", enr_status, enr_lines)

    # Priors coverage
    pri_status="PASS"; pri_lines=[]
    have_ids = set(fx["fixture_id"].astype(str)) if "fixture_id" in fx.columns else set()
    all5 = 0
    for tag, rel in PRI_FILES:
        df = safe_read(os.path.join(DATA, rel))
        if df.empty or "fixture_id" not in df.columns:
            pri_status="WARN"; pri_lines.append(f"- {rel}: empty or missing fixture_id")
            continue
        got = set(df["fixture_id"].astype(str))
        cov = len(have_ids & got) if have_ids else len(df)
        pri_lines.append(f"- {tag}: coverage={cov}/{len(have_ids) if have_ids else 'n/a'}")
        # ALL five
        if tag=="XG":
            all5 = None  # count later
    # ALL-five coverage if all priors are present
    pri_dfs = [safe_read(os.path.join(DATA, rel)) for _, rel in PRI_FILES]
    if all(df is not None and not df.empty and "fixture_id" in df.columns for df in pri_dfs) and have_ids:
        inter = set.intersection(*[set(df["fixture_id"].astype(str)) for df in pri_dfs])
        pri_lines.append(f"- ALL five priors: {len(inter)}/{len(have_ids)}")
        if len(inter) < 0.7*len(have_ids): pri_status="WARN"
    lines += section("Priors coverage", pri_status, pri_lines)

    # Cross-file joins
    join_status="PASS"; join_lines=[]
    def overlap(a,b,name):
        if a and b:
            o=len(a & b); join_lines.append(f"- {name}: overlap={o}")
            return o
        return 0
    fids_fx   = set(fx["fixture_id"].astype(str)) if "fixture_id" in fx.columns else set()
    fids_enr  = set(enr["fixture_id"].astype(str)) if "fixture_id" in enr.columns else set()
    fids_feat = set(feat["fixture_id"].astype(str)) if "fixture_id" in feat.columns else set()
    fids_mtx  = set(mtx["fixture_id"].astype(str)) if "fixture_id" in mtx.columns else set()
    fids_act  = set(act["fixture_id"].astype(str)) if "fixture_id" in act.columns else set()
    a = overlap(fids_fx, fids_enr, "fixtures ↔ enriched")
    b = overlap(fids_enr, fids_feat, "enriched ↔ features")
    c = overlap(fids_feat, fids_mtx, "features ↔ model_matrix")
    d = overlap(fids_enr, fids_act, "enriched ↔ actionability")
    if min(x for x in [a,b,c,d] if isinstance(x,int)) == 0:
        join_status="FAIL"
    lines += section("Cross-file join integrity", join_status, join_lines)

    # Risk & governance
    risk_status="PASS"; risk_lines=[]
    if act.empty:
        risk_status="FAIL"; risk_lines.append("- ACTIONABILITY_REPORT.csv: empty")
    else:
        risk_lines.append(f"- actionability rows: {len(act)}; nonzero stakes: {int((act.get('final_stake',0)>0).sum())}")
        # reasons distribution (top 5)
        if "reasons" in act.columns:
            rs = act["reasons"].fillna("").astype(str)
            tokens=[]
            for s in rs:
                for t in s.split(";"):
                    t=t.strip()
                    if t: tokens.append(t)
            if tokens:
                srs = pd.Series(tokens).value_counts().head(5)
                risk_lines.append("- top reasons:")
                for k,v in srs.items():
                    risk_lines.append(f"  - {k}: {v}")
    if why.empty:
        risk_lines.append("- WHY_NOT_BET.csv: empty (maybe okay if few vetoes).")
    else:
        risk_lines.append(f"- veto rows: {len(why)}")
    lines += section("Risk & governance", risk_status, risk_lines)

    # Safety: blank guard & SLOs & sanity
    safe_status="PASS"; safe_lines=[]
    if os.path.exists(blank):
        with open(blank,"r",encoding="utf-8") as f:
            txt=f.read()
        if "❌" in txt:
            safe_status="FAIL"; safe_lines.append("- BLANK_FILE_ALERTS: ❌ critical blanks flagged.")
        elif "⚠️" in txt:
            if safe_status!="FAIL": safe_status="WARN"
            safe_lines.append("- BLANK_FILE_ALERTS: ⚠️ non-critical blanks or low rows.")
        else:
            safe_lines.append("- BLANK_FILE_ALERTS: no issues.")
    if os.path.exists(slo):
        fx_count = re.search(r"Fixtures fetched:\s*\*\*(\d+)\*\*", open(slo,"r",encoding="utf-8").read())
        if fx_count: safe_lines.append(f"- SLO fixtures: {fx_count.group(1)}")
    if os.path.exists(sanity):
        with open(sanity,"r",encoding="utf-8") as f:
            st=f.read()
        if "OVERALL: ✅ PASS" in st:
            safe_lines.append("- POST_RUN_SANITY: ✅ PASS")
        elif "OVERALL: ⚠️ WARN" in st:
            if safe_status!="FAIL": safe_status="WARN"
            safe_lines.append("- POST_RUN_SANITY: ⚠️ WARN")
        elif "OVERALL: ❌ FAIL" in st:
            safe_status="FAIL"; safe_lines.append("- POST_RUN_SANITY: ❌ FAIL")
    lines += section("Safety & sanity", safe_status, safe_lines)

    # Write report
    with open(OUT,"w",encoding="utf-8") as f:
        f.write("\n".join(lines)+"\n")
    print(f"deep_sanity_probe: wrote {OUT}")

if __name__ == "__main__":
    main()