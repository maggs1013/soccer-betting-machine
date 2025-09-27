#!/usr/bin/env python3
"""
blank_file_guard.py — detect blank/near-blank artifacts and surface them.

Outputs:
  reports/BLANK_FILE_ALERTS.md

Behavior:
- Reports CRITICAL files that are blank (rows==0), missing (-1), or unreadable (-2).
- Reports IMPORTANT files that are blank (rows==0).
- If env BLANK_FAIL_CRITICAL="1", exits non-zero when CRITICAL blanks are found.
"""

import os, sys
import pandas as pd

DATA = "data"
REP  = "reports"
os.makedirs(REP, exist_ok=True)
OUT  = os.path.join(REP, "BLANK_FILE_ALERTS.md")

# Files that must exist and be non-blank for a healthy “betting-ready” run
CRITICAL = [
    "UPCOMING_fixtures.csv",
    "UPCOMING_7D_enriched.csv",
    "UPCOMING_7D_model_matrix.csv",
    "ACTIONABILITY_REPORT.csv",
]

# Files that should be present but can be blank without hard failing the run
IMPORTANT = [
    "PRIORS_XG_SIM.csv",
    "PRIORS_AVAIL.csv",
    "PRIORS_SETPIECE.csv",
    "PRIORS_MKT.csv",
    "PRIORS_UNC.csv",
    "CONSISTENCY_CHECKS.csv",
    "WHY_NOT_BET.csv",
]

def read_rows(relpath: str) -> int:
    path = os.path.join(DATA, relpath)
    if not os.path.exists(path):
        return -1  # missing
    try:
        df = pd.read_csv(path)
        return len(df)
    except Exception:
        return -2  # unreadable / parse error

def main():
    lines = ["# BLANK FILE ALERTS", ""]
    critical_blank = False

    for rel in CRITICAL:
        n = read_rows(rel)
        if n <= 0:
            msg = f"- ❌ **CRITICAL blank**: {rel} (rows={n})"
            if n == -1: msg += " — missing file"
            if n == -2: msg += " — unreadable csv"
            lines.append(msg)
            critical_blank = True
        elif n < 5:
            lines.append(f"- ⚠️ **Low rows**: {rel} (rows={n})")

    for rel in IMPORTANT:
        n = read_rows(rel)
        if n == 0:
            lines.append(f"- ⚠️ **Blank**: {rel} (rows=0)")
        elif n in (-1, -2):
            lines.append(f"- ℹ️ Optional missing/unreadable: {rel} (rows={n})")

    if len(lines) == 2:
        lines.append("- ✅ No blank/near-blank artifacts detected.")

    with open(OUT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"blank_file_guard: wrote {OUT}")

    if critical_blank and os.environ.get("BLANK_FAIL_CRITICAL", "0") == "1":
        sys.exit(2)

if __name__ == "__main__":
    main()