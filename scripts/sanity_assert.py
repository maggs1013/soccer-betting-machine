#!/usr/bin/env python3
"""
sanity_assert.py — small asserts to fail fast when sources are empty

Usage:
  python scripts/sanity_assert.py --mode=connectors
  python scripts/sanity_assert.py --mode=fixtures

Modes:
- connectors: read reports/CONNECTOR_HEALTH.md and require at least one positive count
- fixtures  : read reports/FIXTURES_DEBUG.md and require rows>0 and in-window>0
Env:
- ALLOW_EMPTY_SLATE=1 to bypass failures (not recommended)
"""

import os, sys, re

REP = "reports"

def read(path):
    if not os.path.exists(path):
        return ""
    return open(path, "r", encoding="utf-8").read()

def assert_connectors():
    md = read(os.path.join(REP, "CONNECTOR_HEALTH.md"))
    if not md:
        print("sanity_assert(connectors): CONNECTOR_HEALTH.md missing")
        sys.exit(1)

    # basic heuristics
    af_fixtures = re.search(r"Fixtures next 7d:\s*\*\*(\d+)\*\*", md)
    fd_matches7 = re.search(r"Matches next 7d:\s*\*\*(\d+)\*\*", md)
    fd_matches30= re.search(r"Matches past 30d:\s*\*\*(\d+)\*\*", md)

    af = int(af_fixtures.group(1)) if af_fixtures else 0
    fd7= int(fd_matches7.group(1)) if fd_matches7 else 0
    fd30=int(fd_matches30.group(1)) if fd_matches30 else 0

    ok = (af > 0) or (fd7 + fd30 > 0)
    if not ok and os.environ.get("ALLOW_EMPTY_SLATE","0") != "1":
        print("sanity_assert(connectors): Both API-Football and FD.org returned zero matches. Failing.")
        sys.exit(2)
    print("sanity_assert(connectors): PASS")
    sys.exit(0)

def assert_fixtures():
    md = read(os.path.join(REP, "FIXTURES_DEBUG.md"))
    if not md:
        print("sanity_assert(fixtures): FIXTURES_DEBUG.md missing")
        sys.exit(1)

    rows = re.search(r"has\s+\*\*(\d+)\s*rows\*\*", md)
    inwin = re.search(r"rows in \[now, \+7d\]:\s+\*\*(\d+)\*\*", md)

    total = int(rows.group(1)) if rows else 0
    w = int(inwin.group(1)) if inwin else 0

    ok = (total > 0) and (w > 0)
    if not ok and os.environ.get("ALLOW_EMPTY_SLATE","0") != "1":
        print(f"sanity_assert(fixtures): total={total}, in_window={w}. Failing.")
        sys.exit(2)
    print("sanity_assert(fixtures): PASS")
    sys.exit(0)

def main():
    mode = None
    for i, a in enumerate(sys.argv):
        if a.startswith("--mode="):
            mode = a.split("=",1)[1].strip()
    if mode == "connectors":
        assert_connectors()
    elif mode == "fixtures":
        assert_fixtures()
    else:
        print("Usage: sanity_assert.py --mode=connectors|fixtures")
        sys.exit(1)

if __name__ == "__main__":
    main()