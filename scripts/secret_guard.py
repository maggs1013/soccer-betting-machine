#!/usr/bin/env python3
"""
secret_guard.py — fail fast with a helpful message if required secrets are missing.

By default we require:
  - THE_ODDS_API_KEY
  - API_FOOTBALL_KEY

Optional (warn only if missing):
  - THE_ODDS_API_KEY_BACKUP
  - FDORG_TOKEN

Set ALLOW_MISSING_SECRETS=1 to bypass (not recommended).
"""

import os, sys

REQUIRED = ["THE_ODDS_API_KEY", "API_FOOTBALL_KEY"]
OPTIONAL = ["THE_ODDS_API_KEY_BACKUP", "FDORG_TOKEN"]

def main():
    missing = [s for s in REQUIRED if not os.environ.get(s, "").strip()]
    if missing and os.environ.get("ALLOW_MISSING_SECRETS","0") != "1":
        print("❌ Missing required secrets:", ", ".join(missing))
        print("Go to Repo → Settings → Secrets and variables → Actions → New repository secret.")
        sys.exit(2)

    for s in OPTIONAL:
        if not os.environ.get(s, "").strip():
            print(f"⚠️ Optional secret {s} not set")

    print("✅ secret_guard: all good (or bypassed).")
    return 0

if __name__ == "__main__":
    sys.exit(main())