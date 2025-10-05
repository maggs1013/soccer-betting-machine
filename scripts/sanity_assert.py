#!/usr/bin/env python3
"""
sanity_assert.py — fail fast when sources/fixtures are empty.

Usage:
  python scripts/sanity_assert.py --mode=connectors
  python scripts/sanity_assert.py --mode=fixtures

Bypass (not recommended):
  ALLOW_EMPTY_SLATE=1
"""
import os, sys, re

REP = "reports"

def _read(p):
    return open(p, "r", encoding="utf-8").read() if os.path.exists(p) else ""

def assert_connectors():
    md = _read(os.path.join(REP, "CONNECTOR_HEALTH.md"))
    if not md:
        print("sanity_assert(connectors): CONNECTOR_HEALTH.md missing"); sys.exit(1)

    # Support old and new headings
    af = 0
    fd_next = 0
    fd_past = 0

    m = re.search(r"Fixtures next 7d:\s*\*\*(\d+)\*\*", md)
    if m: af = int(m.group(1))

    m = re.search(r"Matches next 7d:\s*\*\*(\d+)\*\*", md)
    if m: fd_next = int(m.group(1))
    m = re.search(r"Matches past 30d:\s*\*\*(\d+)\*\*", md)
    if m: fd_past = int(m.group(1))

    # Wide-horizon wording (if probe prints different caption)
    if fd_next == 0:
        m = re.search(r"Matches nextN:\s*\*\*(\d+)\*\*", md)
        if m: fd_next = int(m.group(1))
    if fd_past == 0:
        m = re.search(r"Matches pastM:\s*\*\*(\d+)\*\*", md)
        if m: fd_past = int(m.group(1))

    ok = (af > 0) or (fd_next + fd_past > 0)
    if not ok and os.environ.get("ALLOW_EMPTY_SLATE", "0") != "1":
        print(f"sanity_assert(connectors): FAIL (af={af}, fd7/nextN={fd_next}, fd30/pastM={fd_past})")
        sys.exit(2)
    print("sanity_assert(connectors): PASS")
    sys.exit(0)

def assert_fixtures():
    md = _read(os.path.join(REP, "FIXTURES_DEBUG.md"))
    if not md:
        print("sanity_assert(fixtures): FIXTURES_DEBUG.md missing"); sys.exit(1)

    total = 0; inwin = 0
    m = re.search(r"has\s+\*\*(\d+)\s*rows\*\*", md);  total = int(m.group(1)) if m else 0
    m = re.search(r"rows in \[now, \+7d\]:\s*\*\*(\d+)\*\*", md); inwin = int(m.group(1)) if m else 0

    ok = (total > 0) and (inwin > 0)
    if not ok and os.environ.get("ALLOW_EMPTY_SLATE", "0") != "1":
        print(f"sanity_assert(fixtures): FAIL (total={total}, in_window={inwin})"); sys.exit(2)
    print("sanity_assert(fixtures): PASS"); sys.exit(0)

def main():
    mode = None
    for a in sys.argv[1:]:
        if a.startswith("--mode="): mode = a.split("=",1)[1]
    if mode == "connectors": assert_connectors()
    elif mode == "fixtures": assert_fixtures()
    else:
        print("Usage: sanity_assert.py --mode=connectors|fixtures"); sys.exit(1)

if __name__ == "__main__":
    main()