# scripts/data_inventory_report.py
# Prints a compact “what we have” report for the council.

import os
import pandas as pd

DATA = "data"
FILES = {
    "HIST_matches.csv": "HIST backbone (Football-Data)",
    "UPCOMING_fixtures.csv": "Upcoming odds (Manual/Odds API)",
    "UPCOMING_7D_enriched.csv": "Upcoming 7d enriched",
    "xg_metrics_current.csv": "FBR xG current",
    "xg_metrics_last.csv": "FBR xG last",
    "xg_metrics_hybrid.csv": "FBR+FBref hybrid xG",
    "sd_fbref_team_stats.csv": "soccerdata FBref team stats",
    "sd_538_spi.csv": "soccerdata 538 SPI ratings",
    "sd_fd_fixtures.csv": "soccerdata FD fixtures (7d)",
    "api_probe_report.json": "API probe report"
}

def show_csv(path, label):
    if not os.path.exists(path):
        print(f"[MISS] {label}: {path}")
        return
    df = pd.read_csv(path)
    print(f"[OK] {label}: rows={len(df)} cols={len(df.columns)}")
    if "date" in df.columns:
        dd = pd.to_datetime(df["date"], errors="coerce")
        print("     date range:", str(dd.min())[:10], "→", str(dd.max())[:10])
    if "league" in df.columns:
        sample = ", ".join(sorted(df["league"].dropna().astype(str).unique()[:6]))
        print("     leagues:", sample, "...")
    print()

def show_json(path, label):
    if not os.path.exists(path):
        print(f"[MISS] {label}: {path}")
        return
    print(f"[OK] {label}: see file {path}")

for fname, label in FILES.items():
    p = os.path.join(DATA, fname)
    if p.endswith(".json"):
        show_json(p, label)
    else:
        show_csv(p, label)