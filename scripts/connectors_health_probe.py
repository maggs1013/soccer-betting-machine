#!/usr/bin/env python3
"""
connectors_health_probe.py — run smoke tests and write CONNECTOR_HEALTH.md (+ JSON sidecar)

What’s in here:
- Adds a summary table (OK/FAIL + headline counts)
- Surfaces rate-limit telemetry from FD.org smoke (if present)
- Backward + forward compatibility with old/new smoke keys:
    * API-Football: fixtures_total (new) or fixtures_7d_total (old)
    * FD.org      : matches_nextN/matches_pastM (new) or matches_next7d/matches_past30d (old)
- Writes a JSON sidecar with harmonized fields for programmatic use
"""

import os
import sys
import json
from datetime import datetime

# --- Ensure the repo root is on sys.path so `connectors/` can be imported ---
THIS_DIR = os.path.dirname(os.path.abspath(__file__))       # scripts/
REPO_ROOT = os.path.dirname(THIS_DIR)                       # repo root
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# Import the smoke modules
try:
    import connectors.api_football_connect_smoke as api_football
    import connectors.football_data_org_connect_smoke as fdorg
except ModuleNotFoundError as e:
    raise SystemExit(
        f"[FATAL] Could not import connectors.* modules. "
        f"Make sure the repo has a 'connectors/' folder at the root, and optionally an __init__.py in it. "
        f"Error: {e}"
    )

REP = "reports"
os.makedirs(REP, exist_ok=True)
OUT_MD = os.path.join(REP, "CONNECTOR_HEALTH.md")
OUT_JSON = os.path.join(REP, "CONNECTOR_HEALTH.json")

def _errors_block(obj: dict):
    errs = (obj or {}).get("errors") or []
    if not errs:
        return []
    return ["- Errors:"] + [f"  - {e}" for e in errs]

def main():
    ts = datetime.utcnow().isoformat() + "Z"
    lines = ["# CONNECTOR HEALTH", f"_Generated: {ts}_", ""]

    # ---------- Run smokes ----------
    try:
        a = api_football.smoke()
    except Exception as e:
        a = {
            "source":"api_football","ok":False,"leagues":[],
            "fixtures_total":0,"fixtures_7d_total":0,"injuries_14d_total":0,
            "errors":[f"smoke exc={e}"]
        }

    try:
        f = fdorg.smoke()
    except Exception as e:
        f = {
            "source":"football_data_org","ok":False,
            "competitions":0,"matches_nextN":0,"matches_pastM":0,
            "matches_next7d":0,"matches_past30d":0,
            "standings_ok":0,"standings":0,"scorers":0,"errors":[f"smoke exc={e}"]
        }

    # ---------- Harmonize headline counts (old/new keys) ----------
    # API-Football
    af_total = int(a.get("fixtures_total", a.get("fixtures_7d_total", 0)))
    af_ok    = bool(a.get("ok", False))
    af_window_days = int(a.get("fixtures_window_days", a.get("fixtures_window", 7)))

    # FD.org
    fd_next = int(f.get("matches_nextN",  f.get("matches_next7d",  0)))
    fd_past = int(f.get("matches_pastM", f.get("matches_past30d", 0)))
    fd_ok   = bool(f.get("ok", False))

    # ---------- Summary table ----------
    lines += [
        "## Summary", "",
        "| Source | OK | Headline counts |",
        "|---|:---:|---|",
        f"| API-Football | {'✅' if af_ok else '❌'} | fixtures_total={af_total} (window_days={af_window_days}) |",
        f"| Football-Data.org | {'✅' if fd_ok else '❌'} | matches_next={fd_next}, matches_past={fd_past} |",
        ""
    ]
    if not (af_ok or fd_ok):
        lines += ["> **ALERT:** Both sources returned zero matches. Check API keys/quotas or discovery filters.", ""]

    # ---------- Detailed sections ----------
    # API-Football
    leagues_str = ", ".join(a.get("leagues") or []) or "(none)"
    injuries = int(a.get("injuries_14d_total", a.get("injuries_total", 0)))
    lines += [
        "## API-Football", "",
        f"- Leagues: {leagues_str}",
        f"- Fixtures total (window_days={af_window_days}): **{af_total}**",
        f"- Injuries (last 14d): **{injuries}**",
    ]
    lines += _errors_block(a)
    lines.append("")

    # FD.org (include rate-limit telemetry if present)
    rl = f.get("rate_limit") or {}
    comps = int(f.get("competitions", 0))
    # preserve older 'standings' if 'standings_ok' absent
    standings_ok = int(f.get("standings_ok", f.get("standings", 0)))
    scorers = int(f.get("scorers", 0))
    lines += [
        "## Football-Data.org", "",
        f"- Competitions: **{comps}**",
        f"- Matches next (wide or 7d): **{fd_next}**",
        f"- Matches past (wide or 30d): **{fd_past}**",
        f"- Standings endpoints OK: **{standings_ok}** (PL/CL trials)",
        f"- Scorers (PL) count: **{scorers}**",
    ]
    if rl:
        lines += [f"- Rate limit (FD.org): available/min={int(rl.get('avail_min',0))}, used={int(rl.get('used',0))}"]
    lines += _errors_block(f)
    lines.append("")

    # ---------- Write files ----------
    with open(OUT_MD, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"connectors_health_probe: wrote {OUT_MD}")

    # Optional JSON sidecar (harmonized keys)
    payload = {
        "generated": ts,
        "api_football": a,
        "football_data_org": f,
        "summary": {
            "api_football_ok": af_ok,
            "fdorg_ok": fd_ok,
            "fixtures_total": af_total,
            "fixtures_window_days": af_window_days,
            "matches_next": fd_next,
            "matches_past": fd_past
        }
    }
    try:
        with open(OUT_JSON, "w", encoding="utf-8") as jh:
            json.dump(payload, jh, ensure_ascii=False, indent=2)
        print(f"connectors_health_probe: wrote {OUT_JSON}")
    except Exception as e:
        print("connectors_health_probe: JSON sidecar write failed:", e)

if __name__ == "__main__":
    main()