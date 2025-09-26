#!/usr/bin/env python3
"""
FBref team stats via soccerdata (final hardened)
- Multi-slice with synonyms → canonical stat_types supported by your soccerdata
- Index-safe (always reset_index), key-alias mapping, non-key all-NaN drop
- Duplicate-proof suffixing, incremental merges, cache fallback
Outputs:
  data/sd_fbref_team_stats.csv
  data/sd_fbref_team_stats.cache.csv
"""

import os
import time
import pandas as pd

DATA_DIR = "data"
OUT   = os.path.join(DATA_DIR, "sd_fbref_team_stats.csv")
CACHE = os.path.join(DATA_DIR, "sd_fbref_team_stats.cache.csv")

COMP   = os.environ.get("FBREF_LEAGUE", "ENG-Premier League")
SEASON = os.environ.get("FBREF_SEASON", "2024-2025")

# Canonical groups and synonyms (robust to soccerdata versions)
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

# If FBREF_SLICES provided, filter to those canonical keys (not synonyms)
ENV_SLICES = [s.strip() for s in os.environ.get("FBREF_SLICES","").split(",") if s.strip()]
if ENV_SLICES:
    CANONICAL_ORDER = [k for k in CANONICAL_SLICES if k in ENV_SLICES]
else:
    CANONICAL_ORDER = list(CANONICAL_SLICES.keys())

KEYS = ("team", "league", "season")
TEAM_ALIASES   = ("Squad", "squad", "Team", "team_name", "club", "Club")
LEAGUE_ALIASES = ("Comp", "comp", "League", "competition", "Competition")
SEASON_ALIASES = ("Season", "season", "Year", "year")


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _ensure_keys(df: pd.DataFrame) -> pd.DataFrame:
    """Map common aliases -> KEYS and ensure keys exist as columns."""
    # team
    if "team" not in df.columns:
        for alt in TEAM_ALIASES:
            if alt in df.columns:
                df["team"] = df[alt]
                break
    # league
    if "league" not in df.columns:
        for alt in LEAGUE_ALIASES:
            if alt in df.columns:
                df["league"] = df[alt]
                break
    # season
    if "season" not in df.columns:
        for alt in SEASON_ALIASES:
            if alt in df.columns:
                df["season"] = df[alt]
                break
    # ensure all keys exist
    for k in KEYS:
        if k not in df.columns:
            df[k] = None
    return df


def _drop_nonkey_allna(df: pd.DataFrame) -> pd.DataFrame:
    to_drop = [c for c in df.columns if c not in KEYS and df[c].isna().all()]
    return df.drop(columns=to_drop) if to_drop else df


def _suffix_and_dedupe(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
    rename_map = {c: f"{c}_{suffix}" for c in df.columns if c not in KEYS}
    df = df.rename(columns=rename_map)
    seen, new_cols = set(), []
    for c in df.columns:
        nc = c
        while nc in seen:
            nc = f"{nc}_dup"
        new_cols.append(nc); seen.add(nc)
    df.columns = new_cols
    return df


def _clean_slice(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
    """Reset index to columns, normalize, ensure KEYS, drop non-key all-NaN, suffix/dedupe."""
    if df is None or df.empty:
        return pd.DataFrame(columns=list(KEYS))
    # bring any index levels out as columns
    df = df.reset_index()
    df = _normalize_columns(df)
    df = _ensure_keys(df)
    df = _drop_nonkey_allna(df)
    if not any(c for c in df.columns if c not in KEYS):
        return pd.DataFrame(columns=list(KEYS))
    df = _suffix_and_dedupe(df, suffix)
    # keys first
    key_cols = list(KEYS)
    other = [c for c in df.columns if c not in key_cols]
    return df[key_cols + other]


def _try_stat_type(fb, canonical: str) -> pd.DataFrame | None:
    """
    Try all synonyms for a canonical slice until one works.
    Return cleaned df or None if all fail.
    """
    for candidate in CANONICAL_SLICES[canonical]:
        try:
            raw = fb.read_team_season_stats(stat_type=candidate)
        except Exception as e:
            print(f"[FBref] {canonical} via '{candidate}' failed: {e}")
            continue
        if raw is None or raw.empty:
            print(f"[FBref] {canonical} via '{candidate}' empty")
            continue
        df = _clean_slice(raw, canonical)  # suffix by canonical key for stability
        if df is not None and not df.empty and df.drop(columns=list(KEYS)).shape[1] > 0:
            print(f"[FBref] {canonical} resolved via '{candidate}', shape={df.shape}")
            return df
        else:
            print(f"[FBref] {canonical} via '{candidate}' had no usable columns after cleaning")
    return None


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    tries, err, merged = 3, None, None

    for _ in range(tries):
        try:
            import soccerdata as sd
            fb = sd.FBref(leagues=COMP, seasons=[SEASON])

            parts: list[pd.DataFrame] = []
            for canonical in CANONICAL_ORDER:
                sl = _try_stat_type(fb, canonical)
                if sl is not None and not sl.empty:
                    parts.append(sl)

            if parts:
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

    # Keys first
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