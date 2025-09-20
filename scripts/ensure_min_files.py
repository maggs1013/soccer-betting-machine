# scripts/ensure_min_files.py
# Ensure minimal CSVs exist with valid headers so pandas never errors on empty files.
# (Reverted version: DOES NOT create manual_odds.csv)

import os
import pandas as pd

DATA = "data"
os.makedirs(DATA, exist_ok=True)

def ensure_csv(path, header_cols):
    """Create or fix a CSV so it has a valid header row (no EmptyDataError)."""
    if not os.path.exists(path):
        pd.DataFrame(columns=header_cols).to_csv(path, index=False)
        print(f"[OK] created missing {path}")
        return
    # If it exists, verify it has a header; if not, rewrite with header
    try:
        df = pd.read_csv(path)
        if len(df.columns) == 0:
            raise ValueError("no header")
    except Exception:
        pd.DataFrame(columns=header_cols).to_csv(path, index=False)
        print(f"[OK] fixed empty or malformed {path}")

# Minimal schemas for required CSVs (manual_odds.csv intentionally NOT included here)
ensure_csv(os.path.join(DATA, "teams_master.csv"),
           ["team","gk_rating","setpiece_rating","crowd_index"])

ensure_csv(os.path.join(DATA, "stadiums.csv"),
           ["team","stadium","lat","lon"])

ensure_csv(os.path.join(DATA, "ref_baselines.csv"),
           ["ref_name","ref_pen_rate"])

ensure_csv(os.path.join(DATA, "injuries.csv"),
           ["date","team","injury_index"])

ensure_csv(os.path.join(DATA, "lineups.csv"),
           ["date","team","key_att_out","key_def_out","keeper_changed"])

ensure_csv(os.path.join(DATA, "team_name_map.csv"),
           ["raw","canonical"])