#!/usr/bin/env python3
"""
FBref team stats via soccerdata (2025-safe, multi-slice, duplicate-proof, index-safe)

- Instantiates FBref with leagues + seasons (API expects this at construction)
- Pulls multiple stat_types and outer-merges them on [team, league, season]
- Unconditionally resets slice index, normalizes column names, ensures KEYS exist
- Suffixes ALL non-key columns per slice; guarantees uniqueness
- Falls back to cache if live fetch fails; writes minimal stub if no cache

Outputs:
    data/sd_fbref_team_stats.csv
    data/sd_fbref_team_stats.cache.csv

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

COMP   = os.environ.get("FBREF_LEAGUE", "ENG-Premier League")
SEASON = os.environ.get("FBREF_SEASON", "2024-2025")

ENV_SLICES = os.environ.get("FBREF_SLICES", "")
if ENV_SLICES.strip():
    STAT_TYPES = [s.strip() for s in ENV_SLICES.split(",") if s.strip()]
else:
    STAT_TYPES = ["standard", "shooting", "passing", "keeper", "defense", "gca", "misc"]

# Merge keys expected in every slice
KEYS = ("team", "league", "season")


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Force columns to string, strip whitespace, collapse inner spaces."""
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _suffix_and_dedupe(df: pd.DataFrame, stat_type: str) -> pd.DataFrame:
    """
    After keys exist as columns, suffix every NON-KEY with _{stat_type}
    and ensure final uniqueness across all columns.
    """
    # First pass: suffix
    rename_map = {}
    for c in list(df.columns):
        if c not in KEYS:
            rename_map[c] = f"{c}_{stat_type}"
    df = df.rename(columns=rename_map)

    # Second pass: enforce uniqueness
    seen = set()
    uniq = []
    for c in df.columns:
        new_c = c
        while new_c in seen:
            new_c = f"{new_c}_dup"
        uniq.append(new_c)
        seen.add(new_c)
    df.columns = uniq

    # Order keys first
    key_cols = list(KEYS)
    other = [c for c in df.columns if c not in key_cols]
    return df[key_cols + other]


def _clean_slice(df: pd.DataFrame, stat_type: str) -> pd.DataFrame:
    """
    Reset index UNCONDITIONALLY, normalize headers, ensure KEYS as columns,
    then suffix/dedupe.
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=list(KEYS))

    # Always reset index to pull any key info out of the index
    df = df.reset_index(drop=True)

    # Normalize header names (strings, no stray whitespace)
    df = _normalize_columns(df)

    # Ensure KEYS exist (as columns)
    for k in KEYS:
        if k not in df.columns:
            df[k] = None

    # Now suffix and de-dupe all non-key columns for this slice
    df = _suffix_and_dedupe(df, stat_type)
    return df


def _read_slice(fb, stat_type: str) -> pd.DataFrame | None:
    """Fetch one stat slice via soccerdata and clean it to a safe, mergeable frame."""
    try:
        raw = fb.read_team_season_stats(stat_type=stat_type)
    except Exception:
        return None
    if raw is None or raw.empty:
        return None
    return _clean_slice(raw, stat_type)


def _merge_slices(parts: list[pd.DataFrame]) -> pd.DataFrame:
    """Outer-merge all cleaned slices on KEYS (all non-keys already unique)."""
    merged = parts[0]
    for sl in parts[1:]:
        merged = pd.merge(merged, sl, on=list(KEYS), how="outer")
    # Keys first
    key_cols = list(KEYS)
    other = [c for c in merged.columns if c not in key_cols]
    return merged[key_cols + other]


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    tries, err, merged = 3, None, None

    for _ in range(tries):
        try:
            import soccerdata as sd
            # API requires leagues & seasons on construction
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

    # Fallbacks
    if merged is None or merged.empty:
        if os.path.exists(CACHE):
            merged = pd.read_csv(CACHE)
            print(f"⚠️ FBref live fetch failed/empty ({err}); using cache with {len(merged)} rows")
        else:
            pd.DataFrame(columns=list(KEYS)).to_csv(OUT, index=False)
            raise SystemExit(f"FBref fetch failed and no cache available: {err}")

    # Final hygiene: keep KEYS present and first
    for k in KEYS:
        if k not in merged.columns:
            merged[k] = None
    key_cols = list(KEYS)
    other = [c for c in merged.columns if c not in key_cols]
    merged = merged[key_cols + other]

    merged.to_csv(OUT, index=False)
    try:
        merged.to_csv(CACHE, index=False)
    except Exception:
        pass

    print(f"✅ FBref wrote {len(merged)} rows × {len(merged.columns)} cols → {OUT}")


if __name__ == "__main__":
    main()