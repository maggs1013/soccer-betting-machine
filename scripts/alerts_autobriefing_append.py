#!/usr/bin/env python3
"""
Appends warnings to reports/AUTO_BRIEFING.md based on inventory signals.
"""

import os, pandas as pd
from datetime import datetime

REPORTS_DIR = "reports"
INV = os.path.join(REPORTS_DIR, "DATA_INVENTORY_REPORT.csv")
OUT = os.path.join(REPORTS_DIR, "AUTO_BRIEFING.md")

def main():
    os.makedirs(REPORTS_DIR, exist_ok=True)
    lines = []
    if os.path.exists(OUT):
        with open(OUT, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()

    now = datetime.utcnow().isoformat()
    lines.append(f"\n---\n**Source Alerts @ {now} UTC**\n")

    if os.path.exists(INV):
        df = pd.read_csv(INV)
        def emit(msg): lines.append(f"- {msg}")
        # Flag empties
        for _, r in df.iterrows():
            if r.get("status") in ("EMPTY","MISSING","READ_ERR"):
                emit(f"❌ {r['label']} → {r['status']} (rows={r.get('rows')}, stale_days={r.get('stale_days')})")
        # Low fixtures heuristic
        fx = df[df["label"].str.contains("Upcoming fixtures", na=False)]
        if not fx.empty and (fx.iloc[0]["status"] in ("EMPTY","MISSING")):
            emit("🚨 No upcoming fixtures detected — check Odds API key and fallback cache.")
    else:
        lines.append("- ℹ️ DATA_INVENTORY_REPORT.csv not found.")

    with open(OUT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print("Alerts appended to reports/AUTO_BRIEFING.md")

if __name__ == "__main__":
    main()