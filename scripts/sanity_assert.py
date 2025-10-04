#!/usr/bin/env python3
"""
sanity_assert.py — fail fast when sources/fixtures are empty.

Usage:
  python scripts/sanity_assert.py --mode=connectors
  python scripts/sanity_assert.py --mode=fixtures

Modes:
- connectors:
    Reads reports/CONNECTOR_HEALTH.md and requires at least one positive source:
      API-Football fixtures_next7d > 0 OR FD.org matches_{next7d|past30d} > 0
- fixtures:
    Reads reports/FIXTURES_DEBUG.md and requires:
      total rows > 0 AND in-window rows > 0

Bypass (not recommended):
  ALLOW_EMPTY_SLATE=1
"""

import os, sys, re

REP = "reports"

def _read(path: str) -> str:
    if not os.path.exists(path): return ""
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def assert_connectors():
    md = _read(os.path.join(REP, "CONNECTOR_HEALTH.md"))
    if not md:
        print("sanity_assert(connectors): CONNECTOR_HEALTH.md missing")
        sys.exit(1)

    af = 0; fd7 = 0; fd30 = 0
    m = re.search(r"Fixtures next 7d:\s*\*\*(\d+)\*\*", md)
    if m: af = int(m.group(1))
    m = re.search(r"Matches next 7d:\s*\*\*(\d+)\*\*", md)
    if m: fd7 = int(m.group(1))
    m = re.search(r"Matches past 30d:\s*\*\*(\d+)\*\*", md)
    if m: fd30 = int(m.group(1))

    ok = (af > 0) or (fd7 + fd30 > 0)
    if not ok and os.environ.get("ALLOW_EMPTY_SLATE", "0") != "1":
        print(f"sanity_assert(connectors): FAIL (af={af}, fd7={fd7}, fd30={fd30})")
        sys.exit(2)
    print("sanity_assert(connectors): PASS")
    sys.exit(0)

def assert_fixtures():
    md = _read(os.path.join(REP, "FIXTURES_DEBUG.md"))
    if not md:
        print("sanity_assert(fixtures): FIXTURES_DEBUG.md missing")
        sys.exit(1)

    total = 0; inwin = 0
    m = re.search(r"has\s+\*\*(\d+)\s*rows\*\*", md)
    if m: total = int(m.group(1))
    m = re.search(r"rows in \[now, \+7d\]:\s*\*\*(\d+)\*\*", md)
    if m: inwin = int(m.group(1))

    ok = (total > 0) and (inwin > 0)
    if not ok and os.environ.get("ALLOW_EMPTY_SLATE", "0") != "1":
        print(f"sanity_assert(fixtures): FAIL (total={total}, in_window={inwin})")
        sys.exit(2)
    print("sanity_assert(fixtures): PASS")
    sys.exit(0)

def main():
    mode = None
    for a in sys.argv[1:]:
        if a.startswith("--mode="):
            mode = a.split("=", 1)[1]
    if mode == "connectors":
        assert_connectors()
    elif mode == "fixtures":
        assert_fixtures()
    else:
        print("Usage: sanity_assert.py --mode=connectors|fixtures")
        sys.exit(1)

if __name__ == "__main__":
    main()