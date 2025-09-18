# scripts/enrich_features.py
# Robust, beginner-safe enrichment:
# - Normalizes team names using data/team_name_map.csv
# - Merges teams_master (GK/set-piece/crowd), injuries, lineups, ref baselines, stadiums (travel km)
# - Merges xG hybrid (FBR primary + FBref fallback merged earlier) into home_/away_ features
# - Never crashes on empty/missing CSVs (safe_read + ensure_cols)
# - Writes enriched raw files back in place:
#     data/raw_football_data.csv
#     data/raw_theodds_fixtures.csv

import os
import math
import pandas as pd
from pandas.errors import EmptyDataError

DATA_DIR = "data"

# ---------- utilities ----------
def safe_read(path: str) -> pd.DataFrame:
    """Read CSV to DataFrame; if file missing or empty, return empty DF instead of raising."""
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
        # if the file had only newlines or whitespace, df may exist but be headerless
        if not isinstance(df, pd.DataFrame):
            return pd.DataFrame()
        return df
    except (EmptyDataError, Exception):
        return pd.DataFrame()

def ensure_cols(df: pd.DataFrame, defaults: dict) -> pd.DataFrame:
    """Create any missing columns with default values (avoids KeyErrors)."""
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default
    return df

def coalesce(df: pd.DataFrame, base_name: str, fill=0.0) -> pd.DataFrame:
    """
    If merges created base_name_x / base_name_y, coalesce them into base_name
    (prefer y, then x, then base), fill NA with 'fill'.
    """
    x, y = f"{base_name}_x", f"{base_name}_y"
    s = df[base_name] if base_name in df.columns else pd.Series([None]*len(df), index=df.index)
    if x in df.columns:
        s = s.where(s.notna(), df[x])
    if y in df.columns:
        s = s.where(s.notna(), df[y])
    df[base_name] = s.fillna(fill)
    for c in (x, y):
        if c in df.columns:
            df.drop(columns=[c], inplace=True)
    return df

def normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize 'date' to timezone-naive pandas datetime if present."""
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)
    return df

def haversine(lat1, lon1, lat2, lon2):
    """Distance (km) on a sphere between two points."""
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2*R*math.asin(math.sqrt(a))

# ---------- name normalizer ----------
def load_name_map(path: str) -> dict:
    """Load raw→canonical name pairs from team_name_map.csv."""
    m = safe_read(path)
    if m.empty or "raw" not in m.columns or "canonical" not in m.columns:
        return {}
    m = m.dropna(subset=["raw", "canonical"])
    return {str(r.raw).strip(): str(r.canonical).strip() for _, r in m.iterrows()}

def apply_name_map(series: pd.Series, name_map: dict) -> pd.Series:
    """Map raw names to canonical names."""
    return series.apply(lambda x: name_map.get(str(x).strip(), str(x).strip()) if pd.notna(x) else x)

# ---------- merge modules ----------
def merge_team_master(df: pd.DataFrame, teams: pd.DataFrame) -> pd.DataFrame:
    """Merge GK/set-piece/crowd priors from teams_master.csv."""
    if teams.empty:
        # Ensure minimal columns exist even if file is empty
        return ensure_cols(df, {
            "home_gk_rating": 0.6, "away_gk_rating": 0.6,
            "home_setpiece_rating": 0.6, "away_setpiece_rating": 0.6,
            "crowd_index": 0.7
        })
    df = df.merge(teams.add_prefix("home_"), left_on="home_team", right_on="home_team", how="left")
    df = df.merge(teams.add_prefix("away_"), left_on="away_team", right_on="away_team", how="left")

    # crowd_index: if missing, copy home default; else leave existing
    if "crowd_index" not in df.columns or df["crowd_index"].isna().all():
        df["crowd_index"] = df.get("home_crowd_index", 0.7).fillna(0.7)

    # coalesce GK/set-piece ratings
    for side in ("home", "away"):
        df = ensure_cols(df, {f"{side}_gk_rating": None, f"{side}_setpiece_rating": None})
        df = coalesce(df, f"{side}_gk_rating", 0.6)
        df = coalesce(df, f"{side}_setpiece_rating", 0.6)

    df["crowd_index"] = df["crowd_index"].fillna(0.7)
    return df

def apply_ref_rates(df: pd.DataFrame, refs: pd.DataFrame) -> pd.DataFrame:
    """Merge referee penalty baselines if ref_name exists."""
    if not refs.empty and "ref_name" in df.columns:
        df = df.merge(refs, how="left", on="ref_name")
        df = coalesce(df, "ref_pen_rate", 0.30)
    else:
        df = ensure_cols(df, {"ref_pen_rate": 0.30})
        df["ref_pen_rate"] = df["ref_pen_rate"].fillna(0.30)
    return df

def apply_injuries(df: pd.DataFrame, inj: pd.DataFrame) -> pd.DataFrame:
    """Merge injuries.csv into home_/away_ injury indexes."""
    if inj.empty:
        return ensure_cols(df, {"home_injury_index": 0.3, "away_injury_index": 0.3})
    inj = normalize_dates(inj)
    df = normalize_dates(df)
    df = ensure_cols(df, {"home_injury_index": None, "away_injury_index": None})
    df = df.merge(inj.rename(columns={"team": "home_team", "injury_index": "home_injury_index"}),
                  on=["date", "home_team"], how="left")
    df = df.merge(inj.rename(columns={"team": "away_team", "injury_index": "away_injury_index"]),
                  on=["date", "away_team"], how="left")
    df = coalesce(df, "home_injury_index", 0.3)
    df = coalesce(df, "away_injury_index", 0.3)
    return df

def apply_lineup_flags(df: pd.DataFrame, lu: pd.DataFrame) -> pd.DataFrame:
    """Merge T-60 lineup flags (key_att_out, key_def_out, keeper_changed)."""
    if lu.empty:
        return ensure_cols(df, {
            "home_key_att_out": 0, "home_key_def_out": 0, "home_keeper_changed": 0,
            "away_key_att_out": 0, "away_key_def_out": 0, "away_keeper_changed": 0
        })
    lu = normalize_dates(lu)
    df = normalize_dates(df)
    for side in ("home", "away"):
        m = lu.rename(columns={
            "team": f"{side}_team",
            "key_att_out": f"{side}_key_att_out",
            "key_def_out": f"{side}_key_def_out",
            "keeper_changed": f"{side}_keeper_changed"
        })
        df = df.merge(m, on=["date", f"{side}_team"], how="left")
        for flag in (f"{side}_key_att_out", f"{side}_key_def_out", f"{side}_keeper_changed"):
            df[flag] = df.get(flag, 0).fillna(0).astype(int)
    return df

def compute_travel(df: pd.DataFrame, stad: pd.DataFrame) -> pd.DataFrame:
    """Compute away_travel_km using stadium coords; default to 200 if unknown."""
    df = ensure_cols(df, {"home_travel_km": None, "away_travel_km": None})
    if stad.empty:
        df["home_travel_km"] = df["home_travel_km"].fillna(0.0)
        df["away_travel_km"] = df["away_travel_km"].fillna(200.0)
        return df

    sH = stad.add_prefix("home_")
    sA = stad.add_prefix("away_")
    df = df.merge(sH, how="left", left_on="home_team", right_on="home_team")
    df = df.merge(sA, how="left", left_on="away_team", right_on="away_team")

    def row_dist(r):
        if pd.isna(r.get("home_lat")) or pd.isna(r.get("away_lat")):
            return 200.0
        return haversine(r["home_lat"], r["home_lon"], r["away_lat"], r["away_lon"])

    df["home_travel_km"] = df["home_travel_km"].fillna(0.0)
    mask = df["away_travel_km"].isna()
    if mask.any():
        df.loc[mask, "away_travel_km"] = df[mask].apply(row_dist, axis=1)
    return df

def merge_xg_hybrid(df: pd.DataFrame, xgdf: pd.DataFrame) -> pd.DataFrame:
    """Merge team-level xG hybrid (home_/away_)."""
    if xgdf is None or xgdf.empty or "team" not in xgdf.columns:
        # ensure columns exist
        for c in ["home_xg","home_xga","home_xgd","home_xgd_per90","away_xg","away_xga","away_xgd","away_xgd_per90"]:
            if c not in df.columns: df[c] = None
        return df

    # Home
    hxg = xgdf.rename(columns={
        "team": "home_team",
        "xg_hybrid": "home_xg",
        "xga_hybrid": "home_xga",
        "xgd_hybrid": "home_xgd",
        "xgd90_hybrid": "home_xgd_per90"
    })
    df = df.merge(hxg[["home_team","home_xg","home_xga","home_xgd","home_xgd_per90"]], on="home_team", how="left")

    # Away
    axg = xgdf.rename(columns={
        "team": "away_team",
        "xg_hybrid": "away_xg",
        "xga_hybrid": "away_xga",
        "xgd_hybrid": "away_xgd",
        "xgd90_hybrid": "away_xgd_per90"
    })
    df = df.merge(axg[["away_team","away_xg","away_xga","away_xgd","away_xgd_per90"]], on="away_team", how="left")

    # Ensure columns exist even if merge didn’t add them
    for c in ["home_xg","home_xga","home_xgd","home_xgd_per90","away_xg","away_xga","away_xgd","away_xgd_per90"]:
        if c not in df.columns: df[c] = None
    return df

# ---------- main enrichment ----------
def enrich_file(path: str, teams: pd.DataFrame, stad: pd.DataFrame, refs: pd.DataFrame,
                inj: pd.DataFrame, lu: pd.DataFrame, xgdf: pd.DataFrame, name_map: dict):
    """Load a raw file, normalize names, merge features, write back."""
    if not os.path.exists(path):
        return
    df = safe_read(path)
    if df.empty:
        print(f"[INFO] {path} is empty; skipping enrichment for this file.")
        return

    df = normalize_dates(df)

    # Normalize team names in target file
    if "home_team" in df.columns:
        df["home_team"] = apply_name_map(df["home_team"], name_map)
    if "away_team" in df.columns:
        df["away_team"] = apply_name_map(df["away_team"], name_map)

    # Ensure base columns so merges never fail
    df = ensure_cols(df, {
        "home_team": "", "away_team": "",
        "home_odds_dec": None, "draw_odds_dec": None, "away_odds_dec": None,
        "home_rest_days": 4, "away_rest_days": 4,
        "home_injury_index": 0.3, "away_injury_index": 0.3,
        "home_gk_rating": 0.6, "away_gk_rating": 0.6,
        "home_setpiece_rating": 0.6, "away_setpiece_rating": 0.6,
        "ref_pen_rate": 0.30, "crowd_index": 0.7,
        "home_travel_km": 0.0, "away_travel_km": 200.0
    })

    # Merge reference tables (normalize their team names first)
    df = merge_team_master(df, teams)
    df = apply_injuries(df, inj)
    df = apply_lineup_flags(df, lu)
    df = apply_ref_rates(df, refs)
    df = compute_travel(df, stad)
    df = merge_xg_hybrid(df, xgdf)

    df.to_csv(path, index=False)
    print(f"[OK] Enriched {path} with {len(df)} rows")

def main():
    # Load reference tables
    teams = safe_read(os.path.join(DATA_DIR, "teams_master.csv"))
    stad  = safe_read(os.path.join(DATA_DIR, "stadiums.csv"))
    refs  = safe_read(os.path.join(DATA_DIR, "ref_baselines.csv"))
    inj   = safe_read(os.path.join(DATA_DIR, "injuries.csv"))
    lu    = safe_read(os.path.join(DATA_DIR, "lineups.csv"))
    xgdf  = safe_read(os.path.join(DATA_DIR, "xg_metrics_hybrid.csv"))
    name_map = load_name_map(os.path.join(DATA_DIR, "team_name_map.csv"))

    # Normalize team columns in reference tables
    for ref in (teams, stad, inj, lu, xgdf):
        if not ref.empty and "team" in ref.columns:
            ref["team"] = apply_name_map(ref["team"], name_map)

    # Enrich both raw files if they exist
    enrich_file(os.path.join(DATA_DIR, "raw_football_data.csv"), teams, stad, refs, inj, lu, xgdf, name_map)
    enrich_file(os.path.join(DATA_DIR, "raw_theodds_fixtures.csv"), teams, stad, refs, inj, lu, xgdf, name_map)
    print("[DONE] Enrichment complete.")

if __name__ == "__main__":
    main()
