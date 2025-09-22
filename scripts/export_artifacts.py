#!/usr/bin/env python3
import os, zipfile
from datetime import datetime

RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
REQUIRED = [
    "PREDICTIONS_7D.csv",
    "PREDICTIONS_BTTS_7D.csv",
    "PREDICTIONS_TOTALS_7D.csv",
    "CONSISTENCY_CHECKS.csv",
    "ACTIONABILITY_REPORT.csv",
    "COUNCIL_BRIEFING.md",
    "CALIBRATION_SUMMARY.csv",
    "ROI_BY_SLICE.csv",
    "ODDS_COVERAGE_REPORT.csv",
    "DATA_QUALITY_REPORT.csv",
    "ANTI_MODEL_VETOES.csv",
    "FEATURE_IMPORTANCE.csv",
    "FEATURE_DRIFT.csv",
    "LINE_MOVE_LOG.csv",
    "EXECUTION_FEASIBILITY.csv",
    "PER_LEAGUE_BLEND_WEIGHTS.csv"
]

def main():
    missing = [f for f in REQUIRED if not os.path.exists(os.path.join(RUN_DIR, f))]
    if missing:
        raise SystemExit(f"Missing required artifacts: {missing}")

    out_zip = os.path.join(RUN_DIR, "sbm-7d-bundle.zip")
    with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as z:
        for f in REQUIRED:
            z.write(os.path.join(RUN_DIR, f), arcname=f)
    print("Exported bundle:", out_zip)

if __name__ == "__main__":
    main()