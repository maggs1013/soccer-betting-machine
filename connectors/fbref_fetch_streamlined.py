#!/usr/bin/env python3
"""
fbref_fetch_streamlined.py — FBR API fetcher with strict rate-limiting.

- Enforces BOTH a minimum interval between calls (default 6s) and a 10-calls/min cap.
- Reads ephemeral/stored FBR_API_KEY from env (set by fbr_generate_api_key.py or Secrets).
- Fetches a configurable list of endpoints and saves JSON snapshots for downstream normalization.

Env:
  FBR_API_KEY              : required (exported earlier in workflow)
  FBR_BASE_URL             : default "https://fbrapi.com"
  FBR_MIN_INTERVAL_SEC     : default "6"
  FBR_MAX_CALLS_PER_MIN    : default "10"
  FBR_ENDPOINTS            : CSV of paths; defaults below
  FBR_OUT_DIR              : default "data/fbr"
  FBR_TIMEOUT_SEC          : default "30"
  FBR_RETRIES              : default "3"

Defaults for FBR_ENDPOINTS (edit as your provider exposes):
  "/leagues", "/matches/upcoming", "/matches/recent"

Outputs:
  data/fbr/YYYYMMDD_HHMMSS_<slug>.json   one file per endpoint (timestamped)
  data/fbr/_INDEX.csv                    manifest of pulls (endpoint, file, status, rows if array-like)

Notes:
- Uses urllib (no extra deps). If FBR returns rate-limit headers later we can honor them.
- Token-bucket with 60s window + min-interval gate.
"""

import os, sys, time, json, math
import urllib.request
from datetime import datetime, timezone
from collections import deque
import pandas as pd

BASE_URL = os.environ.get("FBR_BASE_URL", "https://fbrapi.com").rstrip("/")
API_KEY  = os.environ.get("FBR_API_KEY", "").strip()
MIN_INTERVAL = float(os.environ.get("FBR_MIN_INTERVAL_SEC", "6"))
MAX_PER_MIN  = int(os.environ.get("FBR_MAX_CALLS_PER_MIN", "10"))
OUT_DIR = os.environ.get("FBR_OUT_DIR", "data/fbr")
TIMEOUT = int(os.environ.get("FBR_TIMEOUT_SEC", "30"))
RETRIES = int(os.environ.get("FBR_RETRIES", "3"))

DEFAULT_ENDPOINTS = ["/leagues", "/matches/upcoming", "/matches/recent"]
ENDPOINTS = [p.strip() for p in os.environ.get("FBR_ENDPOINTS", ",".join(DEFAULT_ENDPOINTS)).split(",") if p.strip()]

os.makedirs(OUT_DIR, exist_ok=True)

def _now_utc():
    return datetime.now(timezone.utc)

class RateLimiter:
    """Both a min-interval gate and a 10-calls/min token bucket."""
    def __init__(self, min_interval_sec=6.0, max_calls_per_min=10):
        self.min_interval = max(0.0, float(min_interval_sec))
        self.max_calls = max(1, int(max_calls_per_min))
        self.last_ts = 0.0
        self.calls = deque()  # timestamps of last minute

    def wait(self):
        # enforce token bucket over last 60s
        now = time.time()
        while self.calls and (now - self.calls[0]) > 60.0:
            self.calls.popleft()

        if len(self.calls) >= self.max_calls:
            sleep_for = 60.0 - (now - self.calls[0]) + 0.01
            if sleep_for > 0:
                time.sleep(sleep_for)
                now = time.time()
                while self.calls and (now - self.calls[0]) > 60.0:
                    self.calls.popleft()

        # enforce min interval
        delta = now - self.last_ts
        if delta < self.min_interval:
            time.sleep(self.min_interval - delta)

        self.last_ts = time.time()
        self.calls.append(self.last_ts)

def safe_get(path, headers=None, retries=3, timeout=30, rl: RateLimiter=None):
    if rl is None:
        rl = RateLimiter(MIN_INTERVAL, MAX_PER_MIN)
    url = f"{BASE_URL}{path}"
    req = urllib.request.Request(url, headers=headers or {}, method="GET")
    for attempt in range(1, retries + 1):
        try:
            rl.wait()
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = resp.read()
                # Try json decode, but return raw text if it fails
                try:
                    return True, json.loads(data.decode("utf-8"))
                except Exception:
                    return True, data.decode("utf-8", errors="replace")
        except Exception as e:
            if attempt >= retries:
                return False, f"{e}"
            # backoff: min_interval * attempt
            time.sleep(MIN_INTERVAL * attempt)
    return False, "unreachable"

def slugify(path):
    s = path.strip().strip("/").replace("/", "_")
    return s or "root"

def detect_rows(payload):
    """Try to infer how many 'rows' the payload has if it's list-like."""
    try:
        if isinstance(payload, list):
            return len(payload)
        if isinstance(payload, dict):
            # try common keys
            for k in ("results", "data", "items", "response"):
                if k in payload and isinstance(payload[k], list):
                    return len(payload[k])
    except Exception:
        pass
    return None

def main():
    if not API_KEY:
        print("[FBR] ERROR: FBR_API_KEY not set. Generate it earlier in the workflow.", file=sys.stderr)
        sys.exit(1)

    headers = {"Authorization": f"Bearer {API_KEY}"}
    rl = RateLimiter(MIN_INTERVAL, MAX_PER_MIN)

    manifest = []

    ts = _now_utc().strftime("%Y%m%d_%H%M%S")
    for path in ENDPOINTS:
        ok, payload = safe_get(path, headers=headers, retries=RETRIES, timeout=TIMEOUT, rl=rl)
        slug = slugify(path)
        out_path = os.path.join(OUT_DIR, f"{ts}_{slug}.json")
        try:
            with open(out_path, "w", encoding="utf-8") as f:
                if isinstance(payload, (dict, list)):
                    json.dump(payload, f, ensure_ascii=False)
                else:
                    f.write(str(payload))
            rows = detect_rows(payload)
            print(f"[FBR] {path} → ok={ok} rows={rows if rows is not None else 'n/a'} file={out_path}")
            manifest.append({"ts": ts, "endpoint": path, "file": out_path, "ok": ok, "rows": rows})
        except Exception as e:
            print(f"[FBR] write failed for {path}: {e}", file=sys.stderr)
            manifest.append({"ts": ts, "endpoint": path, "file": out_path, "ok": False, "rows": None, "error": str(e)})

    # Write manifest
    idx = os.path.join(OUT_DIR, "_INDEX.csv")
    try:
        pd.DataFrame(manifest).to_csv(idx, index=False)
        print(f"[FBR] Manifest written to {idx} (n={len(manifest)})")
    except Exception as e:
        print(f"[FBR] Failed writing manifest: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()