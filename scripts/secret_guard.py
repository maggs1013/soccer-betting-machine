#!/usr/bin/env python3
"""
secret_guard.py — unify secret names, export canonical envs, and fail fast if required ones are missing.

Usage:
  python scripts/secret_guard.py --context=smoke
  python scripts/secret_guard.py --context=preflight

Contexts:
- smoke:     require API_FOOTBALL_KEY (Odds key not required)
- preflight: require API_FOOTBALL_KEY and THE_ODDS_API_KEY

We also map alternate names used by other branches/teams:
- API_FOOTBALL_KEY  <- API_FOOTBALL_KEY OR APIFOOTBALL_KEY
- FDORG_TOKEN       <- FDORG_TOKEN OR FOOTBALLDATA_TOKEN
- THE_ODDS_API_KEY  <- THE_ODDS_API_KEY OR ODDS_API_KEY (legacy)

Bypass (not recommended):
  ALLOW_MISSING_SECRETS=1
"""

import os
import sys

def _export_env(name: str, value: str):
    """Export env var for downstream steps in GitHub Actions."""
    os.environ[name] = value
    ghe = os.environ.get("GITHUB_ENV", "")
    if ghe:
        try:
            with open(ghe, "a", encoding="utf-8") as fh:
                fh.write(f"{name}={value}\n")
        except Exception:
            pass

def _presence(label: str, value: str):
    print(f"{label}: {'SET' if value else 'NOT SET'}")

def main():
    # ---- parse context ----
    context = "smoke"
    for a in sys.argv[1:]:
        if a.startswith("--context="):
            context = a.split("=", 1)[1].strip().lower()
    if context not in ("smoke", "preflight"):
        print("Usage: secret_guard.py --context=smoke|preflight")
        sys.exit(1)

    # ---- read raw secrets (both naming styles) ----
    # API-Football
    api_football_key = (os.environ.get("API_FOOTBALL_KEY") or
                        os.environ.get("APIFOOTBALL_KEY") or "").strip()

    # Football-Data.org (optional)
    fdorg_token = (os.environ.get("FDORG_TOKEN") or
                   os.environ.get("FOOTBALLDATA_TOKEN") or "").strip()

    # Odds (legacy ODDS_API_KEY for safety)
    odds_main = (os.environ.get("THE_ODDS_API_KEY") or
                 os.environ.get("ODDS_API_KEY") or "").strip()

    # ---- export canonical envs so downstream always reads the same names ----
    if api_football_key:
        _export_env("API_FOOTBALL_KEY", api_football_key)
    if fdorg_token:
        _export_env("FDORG_TOKEN", fdorg_token)
    if odds_main:
        _export_env("THE_ODDS_API_KEY", odds_main)

    # ---- presence check (masked: only prints SET/NOT SET) ----
    _presence("API_FOOTBALL_KEY", api_football_key)
    _presence("FDORG_TOKEN", fdorg_token)
    _presence("THE_ODDS_API_KEY", odds_main)

    # ---- decide required secrets by context ----
    required = []
    if context == "smoke":
        required = ["API_FOOTBALL_KEY"]         # only API-Football is strictly required for smoke
    elif context == "preflight":
        required = ["API_FOOTBALL_KEY", "THE_ODDS_API_KEY"]

    missing = []
    for key in required:
        if not os.environ.get(key, "").strip():
            missing.append(key)

    if missing and os.environ.get("ALLOW_MISSING_SECRETS", "0") != "1":
        print("❌ Missing required secrets for context:", context)
        for m in missing:
            if m == "API_FOOTBALL_KEY":
                print("   - API_FOOTBALL_KEY (you can store as API_FOOTBALL_KEY or APIFOOTBALL_KEY)")
            elif m == "THE_ODDS_API_KEY":
                print("   - THE_ODDS_API_KEY (you can store as THE_ODDS_API_KEY or ODDS_API_KEY)")
            else:
                print(f"   - {m}")
        print("Fix: Repo → Settings → Secrets and variables → Actions → New repository secret")
        sys.exit(2)

    print(f"✅ secret_guard({context}): OK")
    return 0

if __name__ == "__main__":
    sys.exit(main())