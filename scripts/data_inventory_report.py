# scripts/data_inventory_report.py
# Hardened version: safe against empty or malformed CSVs.
# Prints inventory of key data files and writes DATA_INVENTORY_REPORT.csv.

import os
import pandas as pd
from pandas.errors import EmptyDataError, ParserError

DATA = "data"
OUT = os.path.join(DATA, "DATA_INVENTORY_REPORT.csv")

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

def safe_read(path):
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_csv(path)
        return df
    except (EmptyDataError, ParserError):
        return pd.DataFrame()
    except Exception as e:
        print(f"[WARN] Failed to read {path}: {e}")
        return pd.DataFrame()

def main():
    rows = []

    for fname, label in FILES.items():
        p = os.path.join(DATA, fname)
        if not os.path.exists(p):
            print(f"[MISS] {label}: {fname}")
            rows.append({"file": fname, "label": label, "status": "MISSING", "rows": 0, "cols": 0})
            continue

        if fname.endswith(".json"):
            size = os.path.getsize(p)
            status = "EMPTY" if size == 0 else "OK"
            print(f"[{status}] {label}: {fname} (json, size={size} bytes)")
            rows.append({"file": fname, "label": label, "status": status, "rows": None, "cols": None})
            continue

        df = safe_read(p)
        if df is None:
            rows.append({"file": fname, "label": label, "status": "MISSING", "rows": 0, "cols": 0})
        elif df.empty:
            print(f"[EMPTY] {label}: {fname}")
            rows.append({"file": fname, "label": label, "status": "EMPTY", "rows": 0, "cols": 0})
        else:
            print(f"[OK] {label}: {fname} rows={len(df)} cols={len(df.columns)}")
            rows.append({"file": fname, "label": label, "status": "OK", "rows": len(df), "cols": len(df.columns)})

    rep = pd.DataFrame(rows)
    rep.to_csv(OUT, index=False)
    print(f"[DONE] wrote {OUT}")

if __name__ == "__main__":
    main()