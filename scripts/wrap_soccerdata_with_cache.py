#!/usr/bin/env python3
"""
wrap_soccerdata_with_cache.py
Run the normal soccerdata fetcher with a safety net:
- On success: copy outputs to data/cache/soccerdata/latest/
- On failure: restore last-good files from cache so downstream won't break

Expected outputs (update if your fetcher writes differently):
  - data/sd_fbref_team_stats.csv
  - data/sd_538_spi.csv
"""

import os, shutil, subprocess, sys, time

DATA = "data"
CACHE_ROOT = os.path.join(DATA, "cache", "soccerdata")
LATEST = os.path.join(CACHE_ROOT, "latest")
STAMP = time.strftime("%Y%m%d_%H%M%S")

OUTS = [
    os.path.join(DATA, "sd_fbref_team_stats.csv"),
    os.path.join(DATA, "sd_538_spi.csv"),
]

def exists_nonempty(p):
    return os.path.exists(p) and os.path.getsize(p) > 0

def ensure_dir(d):
    os.makedirs(d, exist_ok=True)

def copy_many(src_list, dst_dir):
    ensure_dir(dst_dir)
    for src in src_list:
        if exists_nonempty(src):
            shutil.copy(src, os.path.join(dst_dir, os.path.basename(src)))

def restore_many(src_dir, dst_dir):
    ok = False
    for f in OUTS:
        src = os.path.join(src_dir, os.path.basename(f))
        if exists_nonempty(src):
            shutil.copy(src, os.path.join(dst_dir, os.path.basename(f)))
            ok = True
    return ok

def main():
    ensure_dir(CACHE_ROOT)
    ensure_dir(LATEST)

    # 1) Try normal fetch
    print("[INFO] Running fetch_soccerdata.py with safety cache...")
    code = subprocess.call([sys.executable, "scripts/fetch_soccerdata.py"])
    if code == 0 and all(exists_nonempty(p) for p in OUTS):
        # success → update cache
        stamp_dir = os.path.join(CACHE_ROOT, STAMP)
        copy_many(OUTS, stamp_dir)
        copy_many(OUTS, LATEST)
        print("[OK] soccerdata fetch succeeded; cache updated.")
        sys.exit(0)

    # 2) Fallback to cache
    print("[WARN] soccerdata fetch failed or outputs missing; attempting cache restore...")
    if restore_many(LATEST, DATA):
        print("[OK] Restored last-good soccerdata outputs from cache.")
        sys.exit(0)
    else:
        print("[ERROR] No cache available to restore; aborting.")
        sys.exit(1)

if __name__ == "__main__":
    main()