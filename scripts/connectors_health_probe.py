#!/usr/bin/env python3
"""
connectors_health_probe.py — run smoke tests and write CONNECTOR_HEALTH.md (+ JSON sidecar)

What’s in here:
- Adds a summary table (OK/FAIL + headline counts)
- Surfaces rate-limit telemetry from FD.org smoke (if present)
- Safer defaults and clearer error rendering
- Writes a small JSON sidecar for machine use (optional, harmless)

Inputs (via imports):
  connectors.api_football_connect_smoke.smoke()
  connectors.football_data_org_connect_smoke.smoke()

Outputs:
  reports/CONNECTOR_HEALTH.md
  reports/CONNECTOR_HEALTH.json
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
            "fixtures_7d_total":0,"injuries_14d_total":0,
            "errors":[f"smoke exc={e}"]
        }

    try:
        f = fdorg.smoke()
    except Exception as e:
        f = {
            "source":"football_data_org","ok":False,
            "competitions":0,"matches_next7d":0,"matches_past30d":0,
            "standings":0,"scorers":0,"errors":[f"smoke exc={e}"]
        }

    # ---------- Summary table ----------
    af_fix = int(a.get("fixtures_7d_total", 0))
    af_ok  = bool(a.get("ok", False))
    fd_m7  = int(f.get("matches_next7d", 0))
    fd_m30 = int(f.get("matches_past30d", 0))
    fd_ok  = bool(f.get("ok", False))

    lines += [
        "## Summary", "",
        "| Source | OK | Headline counts |",
        "|---|:---:|---|",
        f"| API-Football | {'✅' if af_ok else '❌'} | fixtures_next7d={af_fix} |",
        f"| Football-Data.org | {'✅' if fd_ok else '❌'} | matches_next7d={fd_m7}, matches_past30d={fd_m30} |",
        ""
    ]
    if not (af_ok or fd_ok):
        lines += ["> **ALERT:** Both sources returned zero matches. Check API keys/quotas or discovery filters.", ""]

    # ---------- Detailed sections ----------
    # API-Football
    leagues_str = ", ".join(a.get("leagues") or []) or "(none)"
    lines += [
        "## API-Football", "",
        f"- Leagues: {leagues_str}",
        f"- Fixtures next 7d: **{af_fix}**",
        f"- Injuries last 14d: **{int(a.get('injuries_14d_total',0))}**",
    ]
    lines += _errors_block(a)
    lines.append("")

    # FD.org (include rate-limit telemetry if present)
    rl = f.get("rate_limit") or {}
    lines += [
        "## Football-Data.org", "",
        f"- Competitions: **{int(f.get('competitions',0))}**",
        f"- Matches next 7d: **{fd_m7}**",
        f"- Matches past 30d: **{fd_m30}**",
        f"- Standings endpoints OK: **{int(f.get('standings',0))}** (PL/CL trials)",
        f"- Scorers (PL) count: **{int(f.get('scorers',0))}**"
    ]
    if rl:
        lines += [f"- Rate limit (FD.org): available/min={int(rl.get('avail_min',0))}, used={int(rl.get('used',0))}"]
    lines += _errors_block(f)
    lines.append("")

    # ---------- Write files ----------
    with open(OUT_MD, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"connectors_health_probe: wrote {OUT_MD}")

    # Optional JSON sidecar for programmatic checks (harmless)
    payload = {
        "generated": ts,
        "api_football": a,
        "football_data_org": f,
        "summary": {
            "api_football_ok": af_ok,
            "fdorg_ok": fd_ok,
            "fixtures_next7d": af_fix,
            "matches_next7d": fd_m7,
            "matches_past30d": fd_m30
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