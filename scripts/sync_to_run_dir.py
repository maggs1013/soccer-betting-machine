#!/usr/bin/env python3
import os, shutil
from datetime import datetime

RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
os.makedirs(RUN_DIR, exist_ok=True)

COPY = [
  ("data/PREDICTIONS_7D.csv", "PREDICTIONS_7D.csv"),
  ("data/ACTIONABILITY_REPORT.csv", "ACTIONABILITY_REPORT.csv"),
  ("data/COUNCIL_BRIEFING.md", "COUNCIL_BRIEFING.md"),
  ("data/CALIBRATION_TABLE.csv", "CALIBRATION_TABLE.csv"),
  ("data/CALIBRATION_SUMMARY.csv", "CALIBRATION_SUMMARY.csv"),
  ("data/CALIBRATION_BY_LEAGUE.csv", "CALIBRATION_BY_LEAGUE.csv"),
  ("data/BACKTEST_SUMMARY.csv", "BACKTEST_SUMMARY.csv"),
  ("data/BACKTEST_BY_WEEK.csv", "BACKTEST_BY_WEEK.csv"),
  ("data/ODDS_COVERAGE_REPORT.csv", "ODDS_COVERAGE_REPORT.csv"),
  ("data/DATA_QUALITY_REPORT.csv", "DATA_QUALITY_REPORT.csv"),
  ("data/VERIFY_XG_REPORT.csv", "VERIFY_XG_REPORT.csv"),
  ("data/LEAGUE_XG_TABLE.csv", "LEAGUE_XG_TABLE.csv"),
  ("data/FEATURE_IMPORTANCE.csv", "FEATURE_IMPORTANCE.csv"),
  ("data/ROI_BY_SLICE.csv", "ROI_BY_SLICE.csv"),
  ("data/COVERAGE_TRENDS.csv", "COVERAGE_TRENDS.csv"),
  ("data/ANTI_MODEL_VETOES.csv", "ANTI_MODEL_VETOES.csv"),
  ("data/VETO_HISTORY.csv", "VETO_HISTORY.csv"),
  ("data/PREDICTIONS_BTTS_7D.csv", "PREDICTIONS_BTTS_7D.csv"),
  ("data/PREDICTIONS_TOTALS_7D.csv", "PREDICTIONS_TOTALS_7D.csv"),
  ("data/PER_LEAGUE_BLEND_WEIGHTS.csv", "PER_LEAGUE_BLEND_WEIGHTS.csv")
]

def cp(src, dst):
    if os.path.exists(src):
        shutil.copy(src, os.path.join(RUN_DIR, dst))

for s, d in COPY:
    cp(s, d)
print("Synced legacy artifacts to", RUN_DIR)