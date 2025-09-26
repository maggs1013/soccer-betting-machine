#!/usr/bin/env python3
"""
FBref team stats via soccerdata (2025-safe, multi-slice)
- Instantiates FBref with leagues + seasons (API expects this at construction)
- Pulls multiple stat_types and outer-merges them on [team, league, season]
- Schema-agnostic: suffixes non-key columns by slice to avoid collisions
- Safe: skips unsupported slices; falls back to cache if live fetch fails
- Outputs:
    data/sd_fbref_team_stats.csv          (live merged table)
    data/sd_fbref_team_stats.cache.csv    (last-good cache)

Env (optional):
  FBREF_LEAGUE="ENG-Premier League"
  FBREF_SEASON="2024-2025"
  FBREF_SLICES="standard,shooting,passing,keeper,defense,gca,misc"
"""

import os, time, sys
import pandas as pd

DATA_DIR = "data"
OUT   = os.path.join(DATA_DIR, "sd_fbref_team_stats.csv")
CACHE = os.path.join(DATA_DIR, "sd_fbref_team_stats.cache.csv")

# Defaults (override via env if you want)
COMP   = os.environ.get("FBREF_LEAGUE", "ENG-Premier League")
SEASON = os.environ.get("FBREF_SEASON", "2024-2025")

# You can trim/extend this list; unsupported slices are skipped safely
ENV_SLICES = os.environ.get("FBREF_SLICES", "")
if ENV_SLICES.strip():
    STAT_TYPES = [s.strip() for s in ENV_SLICES.split(",") if s.strip()]
else:
    STAT_TYPES = ["standard", "shooting", "passing", "keeper", "defense", "gca", "misc"]

KEYS = ("team","league","season")

def _read_slice(fb, stat_type: str) -> pd.DataFrame | None:
    """
    Best-effort slice fetch:
      - read_team_season_stats(stat_type=...)
      - ensure KEYS present; add if missing
      - suffix non-key columns with f"_{stat_type}"
    """
    try:
        df = fb.read_team_season_stats(stat_type=stat_type)
    except TypeError:
        # Older soccerdata builds might need different args; skip slice gracefully
        return None
    except Exception:
        return None

    if df is None or df.empty:
        return None

    # Flatten possible index and ensure keys
    df = df.reset_index(drop=False)
    for k in KEYS:
        if k not in df.columns:
            df[k] = None

    # Suffix non-key columns to avoid collisions between slices
    keys_set = set(KEYS)
    rename_map = {c: f"{c}_{stat_type}" for c in df.columns if c not in keys_set}
    df = df.rename(columns=rename_map)
    return df

def _merge_slices(parts: list[pd.DataFrame]) -> pd.DataFrame:
    merged = parts[0]
    for sl in parts[1:]:
        merged = pd.merge(merged, sl, on=list(KEYS), how="outer")
    # Sort columns: keys first, then the rest
    key_cols = list(KEYS)
    other = [c for c in merged.columns if c not in key_cols]
    return merged[key_cols + other]

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    tries, err = 3, None
    merged = None

    for attempt in range(tries):
        try:
            import soccerdata as sd
            # ⚠️ Current API style: pass leagues & seasons at construction
            fb = sd.FBref(leagues=COMP, seasons=[SEASON])
            parts: list[pd.DataFrame] = []

            for st in STAT_TYPES:
                print(f"[FBref] fetching slice stat_type={st} …")
                sl = _read_slice(fb, st)
                if sl is not None and not sl.empty:
                    parts.append(sl)

            if parts:
                merged = _merge_slices(parts)
            break
        except Exception as e:
            err = e
            time.sleep(2)

    # Fallback to cache if live fetch failed or empty
    if merged is None or merged.empty:
        if os.path.exists(CACHE):
            merged = pd.read_csv(CACHE)
            print(f"⚠️ FBref live fetch failed or empty ({err}); using cache with {len(merged)} rows")
        else:
            # Minimal stub to keep pipeline alive
            pd.DataFrame(columns=list(KEYS)).to_csv(OUT, index=False)
            raise SystemExit(f"FBref fetch failed and no cache available: {err}")

    # Final hygiene: ensure KEYS exist and are first
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