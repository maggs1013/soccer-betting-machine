#!/usr/bin/env python3
"""
feature_importance_report.py — per-league feature importance (human-readable)

Primary path:
  - If TRAIN_MATRIX.csv exists (date, league, target + numeric features):
      * For each league with enough rows:
          - Time-sort, take most recent 20% as evaluation window
          - Fit a simple LogisticRegression on older 80%
          - Compute permutation importance on the 20% tail
          - Report Top 15 and Bottom 10 features by mean importance

Fallback path:
  - If TRAIN_MATRIX.csv missing but TRAINING_REPORT.md or STACK_TRAINING_REPORT.md exist,
    extract any "Top 10 permutation importance" blocks and compile a summary.

Outputs:
  reports/FEATURE_IMPORTANCE.md
"""

import os, re, json, numpy as np, pandas as pd
from datetime import datetime

DATA = "data"
REP  = "reports"
os.makedirs(REP, exist_ok=True)

TRAIN_MTX = os.path.join(DATA, "TRAIN_MATRIX.csv")
TRAIN_RPT = os.path.join(REP,  "TRAINING_REPORT.md")
STACK_RPT = os.path.join(REP,  "STACK_TRAINING_REPORT.md")
OUT_MD    = os.path.join(REP,  "FEATURE_IMPORTANCE.md")

def safe_read_csv(p):
    if not os.path.exists(p): return pd.DataFrame()
    try: return pd.read_csv(p)
    except Exception: return pd.DataFrame()

def _write_md(lines):
    with open(OUT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"feature_importance_report: wrote {OUT_MD}")

def _from_reports():
    lines = ["# FEATURE IMPORTANCE", "", "_Source: parsed from TRAINING_REPORT.md / STACK_TRAINING_REPORT.md_", ""]
    parsed_any = False
    rx_section = re.compile(r"^##\s+League:\s*(.+?)\s*$", re.I)
    rx_item    = re.compile(r"^\s*-\s+([A-Za-z0-9_\-\.]+):\s*([0-9\.\-eE]+)\s*$")

    for rpt in [TRAIN_RPT, STACK_RPT]:
        if not os.path.exists(rpt): continue
        with open(rpt, "r", encoding="utf-8") as f:
            text = f.read()
        cur_league = None
        in_top = False
        for line in text.splitlines():
            m = rx_section.match(line)
            if m:
                cur_league = m.group(1).strip()
                in_top = False
                continue
            if "Top 10 permutation importance" in line:
                in_top = True
                lines += [f"## {cur_league}", "", "**Top features**"]
                continue
            if in_top:
                mi = rx_item.match(line)
                if mi:
                    name, val = mi.group(1), mi.group(2)
                    lines.append(f"- {name}: {val}")
                    parsed_any = True
                else:
                    # end block when a non-item line appears
                    if line.strip().startswith("- ") is False and line.strip() != "":
                        in_top = False
        lines.append("")

    if not parsed_any:
        return False
    _write_md(lines)
    return True

def _from_train_matrix():
    df = safe_read_csv(TRAIN_MTX)
    if df.empty or not {"date","league","target"}.issubset(df.columns):
        return False

    # sanitize
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")
    # numeric features only
    drop_cols = {"fixture_id","target","date","league","home_team","away_team"}
    Xcols = [c for c in df.columns if c not in drop_cols and str(df[c].dtype).startswith(("float","int"))]
    if not Xcols:
        return False

    from sklearn.linear_model import LogisticRegression
    from sklearn.inspection import permutation_importance

    lines = ["# FEATURE IMPORTANCE", "", f"_Generated: {datetime.utcnow().isoformat()}Z_", ""]
    for lg, g in df.groupby("league"):
        if len(g) < 200:
            continue
        split = int(len(g)*0.8)
        g_train = g.iloc[:split]
        g_eval  = g.iloc[split:]
        if len(g_eval) < 50:
            continue
        X_tr = g_train[Xcols].values
        y_tr = g_train["target"].values
        X_te = g_eval[Xcols].values
        y_te = g_eval["target"].values

        try:
            model = LogisticRegression(max_iter=200, solver="lbfgs")
            model.fit(X_tr, y_tr)
            pi = permutation_importance(model, X_te, y_te, n_repeats=5, random_state=42)
            imp = pd.DataFrame({"feature": Xcols, "mean": pi.importances_mean, "std": pi.importances_std})
            imp = imp.sort_values("mean", ascending=False)
            top = imp.head(15)
            bottom = imp.tail(10)

            lines += [f"## {lg}", "", "### Top 15 features"]
            for _, r in top.iterrows():
                lines.append(f"- {r['feature']}: {r['mean']:.4f} ± {r['std']:.4f}")
            lines += ["", "### Bottom 10 features"]
            for _, r in bottom.iterrows():
                lines.append(f"- {r['feature']}: {r['mean']:.4f} ± {r['std']:.4f}")
            lines.append("")
        except Exception as e:
            lines += [f"## {lg}", "", f"- Importance computation failed: {e}", ""]

    if len(lines) <= 4:
        return False
    _write_md(lines)
    return True

def main():
    # Try TRAIN_MATRIX path first (fresh, precise)
    if _from_train_matrix():
        return
    # Fallback: parse trainer reports
    if _from_reports():
        return
    # Nothing available
    _write_md([
        "# FEATURE IMPORTANCE", "",
        "- No TRAIN_MATRIX.csv or trainer reports with importance blocks found.",
        "- Skipping; will generate next run when training artifacts are present."
    ])

if __name__ == "__main__":
    main()