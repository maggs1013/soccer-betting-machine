#!/usr/bin/env python3
"""
FBref team stats via soccerdata (2025-safe, multi-slice, duplicate-proof, index-safe, memory-safe)

- Instantiates FBref with leagues + seasons (API expects this at construction)
- Pulls multiple stat_types and outer-merges them on [team, league, season]
- Unconditionally resets slice index, normalizes headers, ensures KEYS exist (maps Squad/Team aliases)
- Drops all-NaN columns from slices, suffixes ALL non-key columns per slice, guarantees uniqueness
- Skips empty/useless slices, logs slice shapes, merges incrementally to reduce memory spikes
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

# Common aliases soccerdata/FBref sometimes use for team
TEAM_ALIASES = ("Squad", "squad", "Team", "team_name", "club", "Club")


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _ensure_keys(df: pd.DataFrame) -> pd.DataFrame:
    """Guarantee team/league/season exist as columns; map common aliases for team."""
    # map team aliases first
    if "team" not in df.columns:
        for alt in TEAM_ALIASES:
            if alt in df.columns:
                df["team"] = df[alt]
                break
    # league/season may be absent in some slices; create placeholder if so
    for k in KEYS:
        if k not in df.columns:
            df[k] = None
    return df


def _suffix_and_dedupe(df: pd.DataFrame, stat_type: str) -> pd.DataFrame:
    """Suffix every NON-KEY with _{stat_type} and enforce uniqueness."""
    rename_map = {c: f"{c}_{stat_type}" for c in list(df.columns) if c not in KEYS}
    df = df.rename(columns=rename_map)

    seen = set()
    new_cols = []
    for c in df.columns:
        nc = c
        while nc in seen:
            nc = f"{nc}_dup"
        new_cols.append(nc)
        seen.add(nc)
    df.columns = new_cols
    return df


def _clean_slice(df: pd.DataFrame, stat_type: str) -> pd.DataFrame:
    """
    Reset index (pull index levels to columns), normalize headers,
    ensure KEYS, drop all-NaN columns, suffix/dedupe non-keys.
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=list(KEYS))

    # Always pull index levels into columns (do NOT drop)
    df = df.reset_index()
    df = _normalize_columns(df)
    df = _ensure_keys(df)

    # Remove all-NaN columns (soccerdata sometimes emits placeholders)
    df = df.dropna(axis=1, how="all")

    # If only keys remain, nothing to use
    if not any(c for c in df.columns if c not in KEYS):
        return pd.DataFrame(columns=list(KEYS))

    df = _suffix_and_dedupe(df, stat_type)

    # Keys first
    key_cols = list(KEYS)
    other = [c for c in df.columns if c not in key_cols]
    return df[key_cols + other]


def _read_slice(fb, stat_type: str) -> pd.DataFrame | None:
    try:
        raw = fb.read_team_season_stats(stat_type=stat_type)
    except Exception as e:
        print(f"[FBref] slice {stat_type} failed: {e}")
        return None

    if raw is None or raw.empty:
        print(f"[FBref] slice {stat_type} is empty")
        return None

    df = _clean_slice(raw, stat_type)
    if df.empty or df.drop(columns=list(KEYS)).shape[1] == 0:
        print(f"[FBref] slice {stat_type} has no usable columns after cleaning")
        return None

    print(f"[FBref] slice {stat_type} shape={df.shape}")
    return df


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    tries, err, merged = 3, None, None

    for _ in range(tries):
        try:
            import soccerdata as sd
            # API expects leagues & seasons on construction
            fb = sd.FBref(leagues=COMP, seasons=[SEASON])

            parts: list[pd.DataFrame] = []
            for st in STAT_TYPES:
                sl = _read_slice(fb, st)
                if sl is not None and not sl.empty:
                    parts.append(sl)

            if parts:
                # Incremental outer-merge to keep memory in check
                merged = parts[0]
                for sl in parts[1:]:
                    merged = pd.merge(merged, sl, on=list(KEYS), how="outer")
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