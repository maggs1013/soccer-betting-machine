# scripts/merge_scraped_odds.py
# Normalize OddsPortal raw files into data/oddsportal_export.csv
# Expected fields per record:
#   date, home_team, away_team, home_odds_dec, draw_odds_dec, away_odds_dec, source
#
# Includes team-name normalization using data/team_name_map.csv so merges later are clean.

import os, glob, pandas as pd

RAW_DIR = "data/oddsportal/raw"
OUT = "data/oddsportal_export.csv"
TEAM_MAP = "data/team_name_map.csv"

EXPECTED = {"date","home_team","away_team","home_odds_dec","draw_odds_dec","away_odds_dec","source"}

def load_any(p):
    try:
        if p.endswith(".json"): return pd.read_json(p)
        return pd.read_csv(p)
    except Exception:
        return pd.DataFrame()

def normalize_cols(df):
    mapping = {
        "home_odds":"home_odds_dec",
        "draw_odds":"draw_odds_dec",
        "away_odds":"away_odds_dec",
        "Home":"home_odds_dec",
        "Draw":"draw_odds_dec",
        "Away":"away_odds_dec",
        "book":"source",
        "bookmaker":"source"
    }
    for k,v in mapping.items():
        if k in df.columns and v not in df.columns:
            df[v] = df[k]
    if "source" not in df.columns:
        df["source"] = "oddsportal"
    return df

def load_team_map(path):
    if not os.path.exists(path): return {}
    tm = pd.read_csv(path)
    if "raw" not in tm.columns or "canonical" not in tm.columns: return {}
    tm = tm.dropna(subset=["raw","canonical"])
    return {str(r.raw).strip(): str(r.canonical).strip() for _, r in tm.iterrows()}

def apply_team_map(series, name_map):
    return series.apply(lambda x: name_map.get(str(x).strip(), str(x).strip()) if pd.notna(x) else x)

def main():
    if not os.path.exists(RAW_DIR):
        print(f"[INFO] {RAW_DIR} not found; skip merge.")
        return

    paths = glob.glob(os.path.join(RAW_DIR, "*.csv")) + glob.glob(os.path.join(RAW_DIR, "*.json"))
    if not paths:
        print(f"[INFO] No raw files in {RAW_DIR}; skip merge.")
        return

    name_map = load_team_map(TEAM_MAP)
    frames = []
    for p in paths:
        df = load_any(p)
        if df.empty:
            print(f"[WARN] {os.path.basename(p)} empty/unreadable; skip."); continue
        df = normalize_cols(df)
        if not EXPECTED.issubset(set(df.columns)):
            print(f"[WARN] {os.path.basename(p)} missing fields; skip."); continue
        # apply team normalizer
        df["home_team"] = apply_team_map(df["home_team"], name_map)
        df["away_team"] = apply_team_map(df["away_team"], name_map)
        # keep only expected columns
        frames.append(df[list(EXPECTED)].copy())

    if not frames:
        print("[INFO] No valid OddsPortal files to merge."); return

    out = pd.concat(frames, ignore_index=True).dropna(subset=["home_team","away_team"])
    # Parse date column to ISO (if string)
    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.tz_localize(None)
    out.to_csv(OUT, index=False)
    print(f"[OK] wrote {OUT} ({len(out)})")

if __name__ == "__main__":
    main()