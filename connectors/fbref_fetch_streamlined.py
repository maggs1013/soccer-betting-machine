#!/usr/bin/env python3
"""
FBref streamlined fetcher (multi-season)
----------------------------------------
- Reads seasons from env FBREF_SEASONS (comma-separated), e.g. "2022-2023,2023-2024,2024-2025"
- For each season and each canonical slice:
    * fetch slice via soccerdata (with synonyms fallback)
    * clean keys (team/league/season), drop non-key all-NaN cols
    * write per-season shard: data/fbref_slice_<slice>__<season>.csv
- After all seasons, consolidates shards => data/fbref_slice_<slice>.csv
- Writes a manifest: data/fbref_slices_manifest.json
- Does NOT do any giant in-memory merges; enrichment reads per-slice CSVs directly.

Env (optional):
  FBREF_LEAGUE="ENG-Premier League"
  FBREF_SEASONS="2022-2023,2023-2024,2024-2025"
  FBREF_SLICES="standard,shooting,passing,keepers,goal_shot_creation,misc"  (subset if desired)
"""

import os, json, glob
import pandas as pd

DATA = "data"
os.makedirs(DATA, exist_ok=True)

COMP     = os.environ.get("FBREF_LEAGUE", "ENG-Premier League")
SEASONS  = [s.strip() for s in os.environ.get("FBREF_SEASONS", "2022-2023,2023-2024,2024-2025").split(",") if s.strip()]

# Canonical slices + synonyms (robust to soccerdata versions)
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

# Optional subset (canonical keys)
ENV_SLICES = [s.strip() for s in os.environ.get("FBREF_SLICES","").split(",") if s.strip()]
CANONICAL_ORDER = [k for k in CANONICAL_SLICES if (not ENV_SLICES or k in ENV_SLICES)]

KEYS = ("team","league","season")
TEAM_ALIASES   = ("Squad", "squad", "Team", "team_name", "club", "Club")
LEAGUE_ALIASES = ("Comp", "comp", "League", "competition", "Competition")
SEASON_ALIASES = ("Season", "season", "Year", "year")

def _norm_cols(df): df.columns=[str(c).strip() for c in df.columns]; return df

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
    to_drop = [c for c in df.columns if c not in KEYS and df[c].isna().all()]
    return df.drop(columns=to_drop) if to_drop else df

def _write_shard(df: pd.DataFrame, canonical: str, season: str) -> bool:
    if df is None or df.empty: return False
    df = df.reset_index()
    df = _norm_cols(df)
    df = _ensure_keys(df)
    # force season column to this season (some slices may omit)
    df["season"] = df["season"].fillna(season).astype(str)
    df = _drop_nonkey_allna(df)
    if not any(c for c in df.columns if c not in KEYS): return False
    out = os.path.join(DATA, f"fbref_slice_{canonical}__{season}.csv")
    df.to_csv(out, index=False)
    print(f"[FBref] saved shard {canonical} {season} → {out} shape={df.shape}")
    return True

def _try_stat_type(fb, canonical: str, season: str) -> bool:
    for candidate in CANONICAL_SLICES[canonical]:
        try:
            raw = fb.read_team_season_stats(stat_type=candidate)
        except Exception as e:
            print(f"[FBref] {canonical}/{season} via '{candidate}' failed: {e}")
            continue
        if raw is None or raw.empty:
            print(f"[FBref] {canonical}/{season} via '{candidate}' empty")
            continue
        if _write_shard(raw, canonical, season):
            print(f"[FBref] {canonical}/{season} resolved via '{candidate}'")
            return True
        else:
            print(f"[FBref] {canonical}/{season} via '{candidate}' no usable columns after cleaning")
    return False

def _consolidate_slice(canonical: str):
    """Combine per-season shards for this slice → data/fbref_slice_<canonical>.csv"""
    shard_glob = os.path.join(DATA, f"fbref_slice_{canonical}__*.csv")
    shard_paths = sorted(glob.glob(shard_glob))
    if not shard_paths:
        # no shards → ensure consolidated file does not linger stale
        out = os.path.join(DATA, f"fbref_slice_{canonical}.csv")
        if os.path.exists(out):
            os.remove(out)
        print(f"[FBref] consolidate {canonical}: no shards found")
        return

    frames = []
    for p in shard_paths:
        try:
            frames.append(pd.read_csv(p))
        except Exception:
            pass
    if not frames:
        print(f"[FBref] consolidate {canonical}: all shards broken/empty")
        return

    # Small vertical concat is safe (few seasons × ~20 teams)
    df = pd.concat(frames, axis=0, ignore_index=True)
    # Keep keys first
    cols = list(df.columns)
    key_cols = [c for c in KEYS if c in cols]
    other = [c for c in cols if c not in key_cols]
    df = df[key_cols + other]
    out = os.path.join(DATA, f"fbref_slice_{canonical}.csv")
    df.to_csv(out, index=False)
    print(f"[FBref] consolidated {canonical} → {out} rows={len(df)} cols={len(df.columns)}")

def main():
    manifest = {"league": COMP, "seasons": SEASONS, "slices": []}
    try:
        import soccerdata as sd
        for season in SEASONS:
            fb = sd.FBref(leagues=COMP, seasons=[season])
            for canonical in CANONICAL_ORDER:
                if _try_stat_type(fb, canonical, season):
                    manifest["slices"] = sorted(set(manifest["slices"] + [canonical]))
    except Exception as e:
        print(f"[FBref] top-level failure: {e}")

    # Consolidate per-slice shards across seasons
    for canonical in CANONICAL_ORDER:
        _consolidate_slice(canonical)

    with open(os.path.join(DATA, "fbref_slices_manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    print(f"[FBref] manifest → data/fbref_slices_manifest.json {manifest}")

if __name__ == "__main__":
    main()