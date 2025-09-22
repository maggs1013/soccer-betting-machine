#!/usr/bin/env python3
import os, subprocess, sys

STEPS = [
    # assume your existing fetch/merge/enrich scripts already run before
    ["python", "scripts/goals_model.py"],
    ["python", "scripts/totals_pricing.py"],
    ["python", "scripts/train_btts_model.py"],
    ["python", "scripts/predict_btts_model.py"],
    ["python", "scripts/consistency_checks.py"],
    ["python", "scripts/kelly_and_caps.py"],
    ["python", "scripts/execution_feasibility.py"],
    ["python", "scripts/feature_drift.py"],
    ["python", "scripts/line_move_log.py"],
    ["python", "scripts/stacking_log.py"],
    ["python", "scripts/council_briefing.py"],
    ["python", "scripts/export_artifacts.py"]
]

def main():
    for cmd in STEPS:
        print("→", " ".join(cmd))
        rc = subprocess.call(cmd)
        if rc != 0:
            sys.exit(rc)
    print("Pipeline complete.")

if __name__ == "__main__":
    main()