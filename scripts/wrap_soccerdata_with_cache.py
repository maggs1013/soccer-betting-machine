#!/usr/bin/env python3
"""
wrap_soccerdata_with_cache.py

Run fetch_soccerdata.py with a safety cache:
- On success: copy any produced outputs to data/cache/soccerdata/latest/
- On partial success: cache the files that succeeded; restore missing ones from cache
- On failure: restore last-good files from cache so downstream keeps running

Expected outputs (adjust if your fetcher writes differently):
  - data/sd_fbref_team_stats.csv
  - data/sd_538_spi.csv
"""

import os
import shutil
import subprocess
import sys
import time

DATA = "data"
CACHE_ROOT = os.path.join(DATA, "cache", "soccerdata")
LATEST = os.path.join(CACHE_ROOT, "latest")
STAMP = time.strftime("%Y%m%d_%H%M%S")
STATUS_FILE = os.path.join(CACHE_ROOT, "last_status.txt")

# Add/modify expected output list if needed
OUTS = [
    os.path.join(DATA, "sd_fbref_team_stats.csv"),
    os.path.join(DATA, "sd_538_spi.csv"),
]

def exists_nonempty(p: str) -> bool:
    return os.path.exists(p) and os.path.getsize(p) > 0

def ensure_dir(d: str) -> None:
    os.makedirs(d, exist_ok=True)

def copy_many(src_list, dst_dir):
    ensure_dir(dst_dir)
    copied = []
    for src in src_list:
        if exists_nonempty(src):
            shutil.copy(src, os.path.join(dst_dir, os.path.basename(src)))
            copied.append(os.path.basename(src))
    return copied

def restore_missing_from_cache(dst_dir):
    """
    Restore any missing expected files from the cache/LATEST directory.
    Returns list of restored filenames.
    """
    restored = []
    for f in OUTS:
        dst = f
        if exists_nonempty(dst):
            continue
        src = os.path.join(LATEST, os.path.basename(f))
        if exists_nonempty(src):
            shutil.copy(src, dst)
            restored.append(os.path.basename(f))
    return restored

def write_status(msg: str):
    ensure_dir(CACHE_ROOT)
    try:
        with open(STATUS_FILE, "w") as f:
            f.write(msg.strip() + "\n")
    except Exception:
        pass

def main():
    ensure_dir(CACHE_ROOT)
    ensure_dir(LATEST)

    print("[INFO] Running fetch_soccerdata.py with safety cache...")
    code = subprocess.call([sys.executable, "scripts/fetch_soccerdata.py"])

    # Determine which outputs were produced this run
    produced = [os.path.basename(p) for p in OUTS if exists_nonempty(p)]
    missing  = [os.path.basename(p) for p in OUTS if not exists_nonempty(p)]

    if code == 0 and produced:
        # We have at least some fresh files; cache them
        stamp_dir = os.path.join(CACHE_ROOT, STAMP)
        copied1 = copy_many(OUTS, stamp_dir)   # snapshot
        copied2 = copy_many(OUTS, LATEST)      # latest pointer
        print(f"[OK] soccerdata fetch produced: {produced}; cached: {copied2 or copied1}")

        if missing:
            # restore only the missing ones from cache
            restored = restore_missing_from_cache(DATA)
            if restored:
                print(f"[INFO] Restored missing from cache: {restored}")
            else:
                print(f"[WARN] Missing outputs not found in cache: {missing}")
        write_status(f"fresh:{','.join(produced)}; restored:{','.join(restored) if missing else ''}")
        sys.exit(0)

    # If fetch failed or produced nothing useful, try full restore from cache
    print("[WARN] soccerdata fetch failed or produced no usable files; restoring from cache...")
    restored = restore_missing_from_cache(DATA)
    if restored:
        print(f"[OK] Restored from cache: {restored}")
        write_status(f"cache_hit:{','.join(restored)}")
        sys.exit(0)

    # No cache to restore
    print("[ERROR] No cache available to restore; aborting.")
    write_status("error:no_cache")
    sys.exit(1)

if __name__ == "__main__":
    main()