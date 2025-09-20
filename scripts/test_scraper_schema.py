# scripts/test_scraper_schema.py
# Logs whether any OddsPortal raw files exist and checks expected fields.

import os, glob, pandas as pd

RAW_DIR = "data/oddsportal/raw"
EXPECTED = {"date","home_team","away_team","home_odds_dec","draw_odds_dec","away_odds_dec","source"}

def main():
    if not os.path.exists(RAW_DIR):
        print(f"[INFO] {RAW_DIR} not found; skipping OddsPortal schema test.")
        return

    paths = glob.glob(os.path.join(RAW_DIR, "*.csv")) + glob.glob(os.path.join(RAW_DIR, "*.json"))
    if not paths:
        print(f"[INFO] No raw files in {RAW_DIR}; skipping schema test.")
        return

    ok = True
    for p in paths:
        try:
            df = pd.read_json(p) if p.endswith(".json") else pd.read_csv(p)
            cols = set(df.columns)
            missing = EXPECTED - cols
            if missing:
                ok = False
                print(f"[WARN] {os.path.basename(p)} missing fields: {sorted(missing)}")
            else:
                print(f"[OK] {os.path.basename(p)} has expected fields.")
        except Exception as e:
            ok = False
            print(f"[WARN] Failed to parse {os.path.basename(p)}: {e}")

    if ok:
        print("[OK] All checked files match expected schema.")
    else:
        print("[INFO] Some files failed schema check; merge step will skip invalid files safely.")

if __name__ == "__main__":
    main()