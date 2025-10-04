#!/usr/bin/env python3
"""
football_data_org_connect_smoke.py — wide-horizon health counts for Football-Data.org (FD.org)

What it checks (kept from historical version)
---------------------------------------------
- competitions
- matches in two windows:
    next N days (default 7)
    past M days (default 30)
- standings (sample: PL, CL)
- scorers (sample: PL)
- rate-limit telemetry (X-Requests-Available-Minute, X-Requests-Used)

New capability
--------------
- Configurable windows via env:
    FDORG_LOOKAHEAD_DAYS (default 7)   → set 120 for a wide forward slate
    FDORG_LOOKBACK_DAYS  (default 30)  → set 120 to scan deeper history
- Global cap to protect quotas:
    FDORG_SMOKE_MAX_MATCHES (default 3000)
- Shared HttpClient: retries, timeouts, 429/Retry-After, min-interval/max-per-minute if needed

Env (optional)
--------------
FDORG_TOKEN               : bearer token for quotas (secret). Tries unauth if missing.
FDORG_LOOKAHEAD_DAYS      : default "7"
FDORG_LOOKBACK_DAYS       : default "30"
FDORG_SMOKE_MAX_MATCHES   : default "3000"
HTTP_TIMEOUT_SEC          : default "40"
HTTP_RETRIES              : default "3"
FDORG_MIN_INTERVAL_SEC / FDORG_MAX_CALLS_PER_MIN : provider rate knobs (usually unnecessary)

Output (printed JSON; consumed by connectors_health_probe.py)
-------------------------------------------------------------
{
  "source": "football_data_org",
  "ok": true/false,
  "competitions": 63,
  "matches_nextN": 1200,
  "matches_pastM": 980,
  "lookahead_days": 120,
  "lookback_days": 120,
  "standings_ok": 2,
  "scorers": 19,
  "errors": [],
  "rate_limit": {"avail_min": 10, "used": 50}
}
"""

import os
import json
import datetime
from connectors.http_client import HttpClient

BASE = "https://api.football-data.org/v4"

def _iso(d): return d.strftime("%Y-%m-%d")
def _today(): return datetime.datetime.utcnow().date()

def _hdr_int(h, key, default=0):
    try:
        return int(h.get(key, default))
    except Exception:
        return default

def smoke():
    token       = os.environ.get("FDORG_TOKEN","").strip()
    lookahead   = int(os.environ.get("FDORG_LOOKAHEAD_DAYS", "7"))
    lookback    = int(os.environ.get("FDORG_LOOKBACK_DAYS",  "30"))
    max_matches = int(os.environ.get("FDORG_SMOKE_MAX_MATCHES", "3000"))
    timeout     = int(os.environ.get("HTTP_TIMEOUT_SEC", "40"))
    retries     = int(os.environ.get("HTTP_RETRIES", "3"))

    http = HttpClient(provider="fdorg", timeout=timeout, retries=retries)
    headers = {"X-Auth-Token": token} if token else {}

    today = _today()
    end   = today + datetime.timedelta(days=lookahead)
    start = today - datetime.timedelta(days=lookback)

    result = {
        "source": "football_data_org",
        "ok": False,
        "competitions": 0,
        "matches_nextN": 0,
        "matches_pastM": 0,
        "lookahead_days": lookahead,
        "lookback_days": lookback,
        "standings_ok": 0,
        "scorers": 0,
        "errors": [],
        "rate_limit": {}
    }

    # --- competitions
    sc, body, hdr = http.get(f"{BASE}/competitions", headers=headers)
    if sc == 200 and isinstance(body, dict):
        result["competitions"] = len(body.get("competitions", []))
    else:
        preview = body if isinstance(body, str) else json.dumps(body)[:160]
        result["errors"].append(f"competitions sc={sc} {str(preview)[:160]}")

    # rate-limit info (if present)
    if isinstance(hdr, dict):
        result["rate_limit"] = {
            "avail_min": _hdr_int(hdr, "X-Requests-Available-Minute", 0),
            "used":      _hdr_int(hdr, "X-Requests-Used", 0),
        }

    # --- matches next window
    sc, body, _ = http.get(f"{BASE}/matches", headers=headers,
                           params={"dateFrom": _iso(today), "dateTo": _iso(end)})
    if sc == 200 and isinstance(body, dict):
        result["matches_nextN"] = min(len(body.get("matches", [])), max_matches)
    else:
        preview = body if isinstance(body, str) else json.dumps(body)[:160]
        result["errors"].append(f"matches_next sc={sc} {str(preview)[:160]}")

    # --- matches past window
    sc, body, _ = http.get(f"{BASE}/matches", headers=headers,
                           params={"dateFrom": _iso(start), "dateTo": _iso(today)})
    if sc == 200 and isinstance(body, dict):
        result["matches_pastM"] = min(len(body.get("matches", [])), max_matches)
    else:
        preview = body if isinstance(body, str) else json.dumps(body)[:160]
        result["errors"].append(f"matches_past sc={sc} {str(preview)[:160]}")

    # --- standings (sample comps: EPL=PL, UCL=CL)
    for comp in ("PL", "CL"):
        sc, body, _ = http.get(f"{BASE}/competitions/{comp}/standings", headers=headers)
        if sc == 200 and isinstance(body, dict):
            result["standings_ok"] += 1
        else:
            preview = body if isinstance(body, str) else json.dumps(body)[:160]
            result["errors"].append(f"standings({comp}) sc={sc} {str(preview)[:160]}")

    # --- scorers (sample: EPL)
    sc, body, _ = http.get(f"{BASE}/competitions/PL/scorers", headers=headers)
    if sc == 200 and isinstance(body, dict):
        result["scorers"] = len(body.get("scorers", []))
    else:
        preview = body if isinstance(body, str) else json.dumps(body)[:160]
        result["errors"].append(f"scorers(PL) sc={sc} {str(preview)[:160]}")

    # green light if we see matches either in nextN or pastM
    result["ok"] = (result["matches_nextN"] + result["matches_pastM"]) > 0

    print(json.dumps(result, indent=2))
    return result

if __name__ == "__main__":
    smoke()