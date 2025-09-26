#!/usr/bin/env python3
"""
FBref streamlined fetcher:
- Fetch each slice, clean it, and save to data/fbref_slice_<canonical>.csv
- Writes a manifest at data/fbref_slices_manifest.json
- NO big in-memory merge here; merging is a separate lightweight step

Env (optional):
  FBREF_LEAGUE, FBREF_SEASON, FBREF_SLICES
"""

import os, json, time
import pandas as pd

DATA = "data"
os.makedirs(DATA, exist_ok=True)

COMP   = os.environ.get("FBREF_LEAGUE", "ENG-Premier League")
SEASON = os.environ.get("FBREF_SEASON", "2024-2025")

CANONICAL_SLICES = {
    "standard":            ["standard"],
    "shooting":            ["shooting"],
    "passing":             ["passing"],
    "passing_types":       ["passing_types", "pass_types", "passing-types"],
    "defense":             ["defense", "defending", "defensive_actions"],
    "possession":          ["possession"],
    "playing_time":        ["playing_time", "time"],
    "keepers":             ["keepers", "keeper", "gk"],
    "keepers_adv":         ["keepers_adv", "keeper_adv", "keepers-adv", "gk_adv"],
    "goal_shot_creation":  ["goal_shot_creation", "gca"],
    "misc":                ["misc", "miscellaneous"],
}

ENV_SLICES = [s.strip() for s in os.environ.get("FBREF_SLICES","").split(",") if s.strip()]
CANONICAL_ORDER = [k for k in CANONICAL_SLICES.keys() if (not ENV_SLICES or k in ENV_SLICES)]

KEYS = ("team","league","season")
TEAM_ALIASES   = ("Squad", "squad", "Team", "team_name", "club", "Club")
LEAGUE_ALIASES = ("Comp", "comp", "League", "competition", "Competition")
SEASON_ALIASES = ("Season", "season", "Year", "year")

def _norm_cols(df): df.columns = [str(c).strip() for c in df.columns]; return df
def _ensure_keys(df):
    if "team" not in df.columns:
        for alt in TEAM_ALIASES:
            if alt in df.columns: df["team"] = df[alt]; break
    if "league" not in df.columns:
        for alt in LEAGUE_ALIASES:
            if alt in df.columns: df["league"] = df[alt]; break
    if "season" not in df.columns:
        for alt in SEASON_ALIASES:
            if alt in df.columns: df["season"] = df[alt]; break
    for k in KEYS:
        if k not in df.columns: df[k] = None
    return df

def _drop_nonkey_allna(df):
    nonkey_allna = [c for c in df.columns if c not in KEYS and df[c].isna().all()]
    return df.drop(columns=nonkey_allna) if nonkey_allna else df

def _clean_slice_to_disk(df: pd.DataFrame, canonical: str) -> bool:
    if df is None or df.empty: return False
    df = df.reset_index()
    df = _norm_cols(df)
    df = _ensure_keys(df)
    df = _drop_nonkey_allna(df)
    if not any(c for c in df.columns if c not in KEYS): return False
    # we DO NOT suffix here; suffixing was for multi-slice merge conflicts.
    # since we write per-slice CSVs, names stay as-is (merger will handle suffixing).
    out = os.path.join(DATA, f"fbref_slice_{canonical}.csv")
    df.to_csv(out, index=False)
    print(f"[FBref] saved slice {canonical} → {out} shape={df.shape}")
    return True

def _try_stat_type(fb, canonical: str) -> bool:
    for candidate in CANONICAL_SLICES[canonical]:
        try:
            raw = fb.read_team_season_stats(stat_type=candidate)
        except Exception as e:
            print(f"[FBref] {canonical} via '{candidate}' failed: {e}"); continue
        if raw is None or raw.empty:
            print(f"[FBref] {canonical} via '{candidate}' empty"); continue
        ok = _clean_slice_to_disk(raw, canonical)
        if ok:
            print(f"[FBref] {canonical} resolved via '{candidate}'")
            return True
        else:
            print(f"[FBref] {canonical} via '{candidate}' no usable columns after cleaning")
    return False

def main():
    manifest = {"league": COMP, "season": SEASON, "slices": []}
    try:
        import soccerdata as sd
        fb = sd.FBref(leagues=COMP, seasons=[SEASON])
        for canonical in CANONICAL_ORDER:
            if _try_stat_type(fb, canonical):
                manifest["slices"].append(canonical)
    except Exception as e:
        print(f"[FBref] top-level failure: {e}")

    with open(os.path.join(DATA,"fbref_slices_manifest.json"),"w",encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    print(f"[FBref] manifest → data/fbref_slices_manifest.json {manifest}")

if __name__ == "__main__":
    main()