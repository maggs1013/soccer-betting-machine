#!/usr/bin/env python3
"""
secret_guard.py — fail fast if required secrets are missing (with helpful hints).

Required:
  - THE_ODDS_API_KEY
  - API_FOOTBALL_KEY

Optional (warn only):
  - THE_ODDS_API_KEY_BACKUP
  - FDORG_TOKEN

Bypass (not recommended):
  - ALLOW_MISSING_SECRETS=1
"""

import os, sys

REQUIRED = ["THE_ODDS_API_KEY", "API_FOOTBALL_KEY"]
OPTIONAL = ["THE_ODDS_API_KEY_BACKUP", "FDORG_TOKEN"]

def main():
    missing = [s for s in REQUIRED if not os.environ.get(s, "").strip()]
    if missing and os.environ.get("ALLOW_MISSING_SECRETS", "0") != "1":
        print("❌ Missing required secrets:", ", ".join(missing))
        print("Fix: Repo → Settings → Secrets and variables → Actions → New repository secret")
        print("Name must match exactly (e.g., THE_ODDS_API_KEY).")
        sys.exit(2)

    for s in OPTIONAL:
        if not os.environ.get(s, "").strip():
            print(f"⚠️ Optional secret {s} not set (ok unless you need it).")

    print("✅ secret_guard: all required secrets present (or bypassed).")
    return 0

if __name__ == "__main__":
    sys.exit(main())