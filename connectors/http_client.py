#!/usr/bin/env python3
"""
http_client.py — reusable HTTP client with:
- Rate limiting (min-interval + max-calls-per-minute)
- Retries with exponential backoff
- 429 handling with Retry-After
- JSON convenience + robust error logs

Env knobs (defaults are safe):
  GLOBAL_MIN_INTERVAL_SEC   = "0"     # seconds, minimum gap between calls
  GLOBAL_MAX_CALLS_PER_MIN  = "120"   # cap in rolling minute

Per-provider overrides (optional):
  ODDS_MIN_INTERVAL_SEC, ODDS_MAX_CALLS_PER_MIN
  APIFOOTBALL_MIN_INTERVAL_SEC, APIFOOTBALL_MAX_CALLS_PER_MIN
  FDORG_MIN_INTERVAL_SEC, FDORG_MAX_CALLS_PER_MIN
  FBR_MIN_INTERVAL_SEC, FBR_MAX_CALLS_PER_MIN

Use:
  from connectors.http_client import HttpClient
  http = HttpClient(provider="apifootball")  # or "odds", "fdorg", "fbr"
  sc, js, hdr = http.get(url, headers=..., params=...)
"""

import os, time, json, math
import requests
from collections import deque

def _get_limit(name, default):
    val = os.environ.get(name, "").strip()
    try:
        return float(val) if "." in val else int(val)
    except Exception:
        return default

def _provider_limits(provider):
    p = (provider or "").lower()
    # base/global defaults
    min_gap = _get_limit("GLOBAL_MIN_INTERVAL_SEC", 0)
    max_min = _get_limit("GLOBAL_MAX_CALLS_PER_MIN", 120)
    # provider overrides
    if p == "odds":
        min_gap = _get_limit("ODDS_MIN_INTERVAL_SEC", min_gap)
        max_min = _get_limit("ODDS_MAX_CALLS_PER_MIN", max_min)
    elif p in ("apifootball","api_football","api-football"):
        min_gap = _get_limit("APIFOOTBALL_MIN_INTERVAL_SEC", min_gap)
        max_min = _get_limit("APIFOOTBALL_MAX_CALLS_PER_MIN", max_min)
    elif p in ("fdorg","football-data","football_data"):
        min_gap = _get_limit("FDORG_MIN_INTERVAL_SEC", min_gap)
        max_min = _get_limit("FDORG_MAX_CALLS_PER_MIN", max_min)
    elif p in ("fbr","fbref","fbref-api"):
        min_gap = _get_limit("FBR_MIN_INTERVAL_SEC", min_gap)
        max_min = _get_limit("FBR_MAX_CALLS_PER_MIN", max_min)
    return max(0.0, float(min_gap)), max(1, int(max_min))

class RateLimiter:
    def __init__(self, min_interval_sec=0.0, max_calls_per_min=120):
        self.min_interval = float(min_interval_sec)
        self.max_calls = int(max_calls_per_min)
        self.last_ts = 0.0
        self.calls = deque()

    def wait(self):
        now = time.time()
        # clear old timestamps (older than 60s)
        while self.calls and now - self.calls[0] > 60.0:
            self.calls.popleft()

        # token bucket: if we're at max, sleep until a token frees
        if len(self.calls) >= self.max_calls:
            sleep_for = 60.0 - (now - self.calls[0]) + 0.01
            if sleep_for > 0:
                time.sleep(sleep_for)
                now = time.time()
                while self.calls and now - self.calls[0] > 60.0:
                    self.calls.popleft()

        # min-interval
        delta = now - self.last_ts
        if delta < self.min_interval:
            time.sleep(self.min_interval - delta)

        self.last_ts = time.time()
        self.calls.append(self.last_ts)

class HttpClient:
    def __init__(self, provider="", timeout=30, retries=3, session=None):
        min_gap, max_min = _provider_limits(provider)
        self.rl = RateLimiter(min_gap, max_min)
        self.timeout = int(os.environ.get("HTTP_TIMEOUT_SEC", timeout))
        self.retries = int(os.environ.get("HTTP_RETRIES", retries))
        self.session = session or requests.Session()

    def _request(self, method, url, **kw):
        backoff_base = max(1.0, float(os.environ.get("HTTP_BACKOFF_BASE", "1.0")))
        for attempt in range(1, self.retries + 1):
            try:
                self.rl.wait()
                resp = self.session.request(method, url, timeout=self.timeout, **kw)
                # Retry-After handling for 429
                if resp.status_code == 429:
                    ra = resp.headers.get("Retry-After")
                    try:
                        sleep_for = float(ra)
                    except Exception:
                        sleep_for = max(3.0, 2.0 * attempt)
                    time.sleep(sleep_for)
                    continue
                return resp
            except requests.RequestException as e:
                if attempt >= self.retries:
                    raise
                time.sleep(backoff_base * attempt)
        # should never reach
        raise RuntimeError("http_client: retries exhausted")

    def get(self, url, headers=None, params=None):
        resp = self._request("GET", url, headers=headers, params=params)
        return self._result(resp)

    def post(self, url, headers=None, json=None, data=None):
        resp = self._request("POST", url, headers=headers, json=json, data=data)
        return self._result(resp)

    @staticmethod
    def _result(resp):
        sc = resp.status_code
        hdr = resp.headers
        ct = hdr.get("content-type", "")
        if ct.startswith("application/json"):
            try:
                return sc, resp.json(), hdr
            except Exception:
                return sc, {"_raw": resp.text}, hdr
        return sc, resp.text, hdr