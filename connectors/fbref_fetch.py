#!/usr/bin/env python3
"""
FBref team stats via soccerdata (2025-safe)
- Instantiate FBref with leagues + seasons
- Call read_team_season_stats(stat_type=...)  (no seasons/competition kwargs here)
- Pull multiple stat types and outer-merge on [team, league, season]
- Cache last-good to data/sd_fbref_team_stats.cache.csv
- Write live to data/sd_fbref_team_stats.csv
"""

import os, time
import pandas as pd

DATA_DIR = "data"
OUT   = os.path.join(DATA_DIR, "sd_fbref_team_stats.csv")
CACHE = os.path.join(DATA_DIR, "sd_fbref_team_stats.cache.csv")

# You can change these or promote to env vars later.
COMP   = os.environ.get("FBREF_LEAGUE", "ENG-Premier League")
SEASON = os.environ.get("FBREF_SEASON", "2024-2025")
STAT_TYPES = ["standard", "shooting", "passing", "keeper"]  # skip silently if one is unsupported

def fetch_stat_slice(fb, stat_type: str) -> pd.DataFrame | None:
    """Fetch one stat_type slice and return normalized dataframe."""
    try:
        df = fb.read_team_season_stats(stat_type=stat_type)
        if df is None or df.empty:
            return None
        # Ensure flat index
        df = df.reset_index(drop=False)
        # Normalize keys if present, else add
        for k in ("team", "league", "season"):
            if k not in df.columns:
                df[k] = None
        # Suffix non-key columns to avoid collisions
        suffix = f"_{stat_type}"
        keep_keys = {"team","league","season"}
        df = df.rename(columns={c: f"{c}{suffix}" for c in df.columns if c not in keep_keys})
        return df
    except Exception:
        return None

def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    # NEW: pass leagues & seasons to constructor (API changed)
    tries, err, merged = 3, None, None
    for _ in range(tries):
        try:
            import soccerdata as sd
            fb = sd.FBref(leagues=COMP, seasons=[SEASON])  # <-- important
            parts: list[pd.DataFrame] = []
            for st in STAT_TYPES:
                print(f"[FBref] fetching stat_type={st} …")
                sl = fetch_stat_slice(fb, st)
                if sl is not None:
                    parts.append(sl)

            if parts:
                merged = parts[0]
                for sl in parts[1:]:
                    merged = pd.merge(merged, sl, on=["team","league","season"], how="outer")
            break
        except Exception as e:
            err = e
            time.sleep(2)

    if merged is None or merged.empty:
        if os.path.exists(CACHE):
            merged = pd.read_csv(CACHE)
            print(f"⚠️ FBref fetch failed ({err}); using cache with {len(merged)} rows")
        else:
            # minimal stub to keep pipeline alive
            pd.DataFrame(columns=["team","league","season"]).to_csv(OUT, index=False)
            raise SystemExit(f"FBref fetch failed and no cache available: {err}")

    merged.to_csv(OUT, index=False)
    try:
        merged.to_csv(CACHE, index=False)
    except Exception:
        pass
    print(f"✅ FBref wrote {len(merged)} rows → {OUT}")

if __name__ == "__main__":
    main()