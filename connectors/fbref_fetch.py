#!/usr/bin/env python3
"""
FBref team stats via soccerdata (2025-safe, multi-slice with duplicate-proof columns)

- Instantiates FBref with leagues + seasons (API expects this at construction)
- Pulls multiple stat_types and outer-merges them on [team, league, season]
- Schema-agnostic: every NON-KEY column is suffixed by slice name and made UNIQUE
- Safe: skips unsupported slices; falls back to cache if live fetch fails

Outputs:
    data/sd_fbref_team_stats.csv          (live merged table)
    data/sd_fbref_team_stats.cache.csv    (last-good cache)

Env (optional):
  FBREF_LEAGUE="ENG-Premier League"
  FBREF_SEASON="2024-2025"
  FBREF_SLICES="standard,shooting,passing,keeper,defense,gca,misc"
"""

import os
import time
import pandas as pd

DATA_DIR = "data"
OUT   = os.path.join(DATA_DIR, "sd_fbref_team_stats.csv")
CACHE = os.path.join(DATA_DIR, "sd_fbref_team_stats.cache.csv")

# Defaults (override via env if you want)
COMP   = os.environ.get("FBREF_LEAGUE", "ENG-Premier League")
SEASON = os.environ.get("FBREF_SEASON", "2024-2025")

# Slice list (ENV overrides default)
ENV_SLICES = os.environ.get("FBREF_SLICES", "")
if ENV_SLICES.strip():
    STAT_TYPES = [s.strip() for s in ENV_SLICES.split(",") if s.strip()]
else:
    STAT_TYPES = ["standard", "shooting", "passing", "keeper", "defense", "gca", "misc"]

# Merge keys expected in every slice
KEYS = ("team", "league", "season")


def _suffix_and_dedupe(df: pd.DataFrame, stat_type: str) -> pd.DataFrame:
    """
    Suffix every NON-KEY column with _{stat_type} and ensure uniqueness
    (handles cases like 'url', 'players_used', etc., appearing in many slices).
    """
    # Ensure keys present
    for k in KEYS:
        if k not in df.columns:
            df[k] = None

    # Flatten any index
    if isinstance(df.index, pd.MultiIndex) or not df.index.equals(pd.RangeIndex(len(df))):
        df = df.reset_index(drop=True)

    # First pass: suffix non-keys
    rename_map = {}
    for c in list(df.columns):
        if c not in KEYS:
            rename_map[c] = f"{c}_{stat_type}"
    df = df.rename(columns=rename_map)

    # Second pass: enforce uniqueness (rare, but protects against upstream duplicates)
    seen = set()
    unique_cols = []
    for c in df.columns:
        new_c = c
        while new_c in seen:
            new_c = f"{new_c}_dup"
        unique_cols.append(new_c)
        seen.add(new_c)
    df.columns = unique_cols

    # Keep KEYS first for readability
    key_cols = list(KEYS)
    other = [c for c in df.columns if c not in key_cols]
    return df[key_cols + other]


def _read_slice(fb, stat_type: str) -> pd.DataFrame | None:
    """Best-effort slice fetch with duplicate-proof renaming."""
    try:
        df = fb.read_team_season_stats(stat_type=stat_type)
    except Exception:
        return None

    if df is None or df.empty:
        return None

    df = _suffix_and_dedupe(df, stat_type)
    return df


def _merge_slices(parts: list[pd.DataFrame]) -> pd.DataFrame:
    """Outer-merge all slice frames on KEYS (columns already unique)."""
    merged = parts[0]
    for sl in parts[1:]:
        merged = pd.merge(merged, sl, on=list(KEYS), how="outer")
    # Final ordering: keys first
    key_cols = list(KEYS)
    other = [c for c in merged.columns if c not in key_cols]
    return merged[key_cols + other]


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    tries, err, merged = 3, None, None

    for _ in range(tries):
        try:
            import soccerdata as sd
            # Current API expects leagues/seasons at construction
            fb = sd.FBref(leagues=COMP, seasons=[SEASON])

            parts: list[pd.DataFrame] = []
            for st in STAT_TYPES:
                print(f"[FBref] fetching slice: {st}")
                sl = _read_slice(fb, st)
                if sl is not None and not sl.empty:
                    parts.append(sl)

            if parts:
                merged = _merge_slices(parts)
            break
        except Exception as e:
            err = e
            time.sleep(2)

    # Fallback: cache or stub
    if merged is None or merged.empty:
        if os.path.exists(CACHE):
            merged = pd.read_csv(CACHE)
            print(f"⚠️ FBref live fetch failed/empty ({err}); using cache with {len(merged)} rows")
        else:
            pd.DataFrame(columns=list(KEYS)).to_csv(OUT, index=False)
            raise SystemExit(f"FBref fetch failed and no cache available: {err}")

    # Hygiene: ensure KEYS exist and are first
    for k in KEYS:
        if k not in merged.columns:
            merged[k] = None
    key_cols = list(KEYS)
    other = [c for c in merged.columns if c not in key_cols]
    merged = merged[key_cols + other]

    # Persist live + cache
    merged.to_csv(OUT, index=False)
    try:
        merged.to_csv(CACHE, index=False)
    except Exception:
        pass

    print(f"✅ FBref wrote {len(merged)} rows × {len(merged.columns)} cols → {OUT}")


if __name__ == "__main__":
    main()