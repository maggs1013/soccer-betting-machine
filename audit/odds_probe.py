import argparse, math, sys
from datetime import datetime, timezone
import pandas as pd
from utils import http_get, write_json, horizon_dates
from config import TARGET_LEAGUES, FUTURE_BUCKETS

BASE = "https://api.the-odds-api.com/v4"

def bucketize_days(dt_iso: str, now_utc: datetime) -> int:
    # Returns whole days from now (>=0). If parse fails, returns 999.
    try:
        dt = datetime.fromisoformat(dt_iso.replace("Z","+00:00")).astimezone(timezone.utc)
        delta = dt - now_utc
        days = math.floor(delta.total_seconds()/86400.0)
        return max(0, days)
    except Exception:
        return 999

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--key", required=True)
    ap.add_argument("--config", default="audit/config.py")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    meta = {
        "ok": True,
        "generated_at_utc": now.isoformat(),
        "rate_limit": {},
        "leagues": {},
        "future_odds_counts": {f"{d}d": 0 for d in FUTURE_BUCKETS},
    }

    # Fetch sports index to verify connectivity
    resp = http_get(f"{BASE}/sports", params={"apiKey": args.key})
    if not hasattr(resp, "status_code") or resp.status_code != 200:
        meta["ok"] = False
        meta["error"] = f"Sports index failed: {getattr(resp,'status_code',None)}"
        write_json(meta, args.out)
        return

    # Optional: rate limit info
    try:
        rl = {
          "remaining": resp.headers.get("x-requests-remaining"),
          "used": resp.headers.get("x-requests-used"),
          "reset": resp.headers.get("x-requests-reset")
        }
        meta["rate_limit"] = rl
    except Exception:
        pass

    # Probe each target league for events and markets
    for lg in TARGET_LEAGUES:
        league_stats = {
            "events": 0,
            "by_bucket": {f"{d}d": 0 for d in FUTURE_BUCKETS},
            "bookmaker_dispersion_avg": None,
            "bookmaker_dispersion_median": None,
            "has_opening_odds": 0,
            "has_closing_odds": 0,
            "errors": None,
            "sample_event": None
        }
        try:
            # List events with included odds
            ev = http_get(f"{BASE}/sports/{lg}/odds", params={
                "apiKey": args.key,
                "regions": "eu,uk,us",
                "markets": "h2h,totals,btts,spreads",
                "oddsFormat": "decimal",
                "dateFormat": "iso"
            }, timeout=30, tries=2)
            if hasattr(ev, "status_code") and ev.status_code == 200:
                events = ev.json()
                league_stats["events"] = len(events)
                dispersions = []

                for e in events:
                    # bucket counts by time-to-kick
                    start = e.get("commence_time")
                    d = bucketize_days(start, now)
                    for B in FUTURE_BUCKETS:
                        if d <= B:  # counts if within bucket
                            league_stats["by_bucket"][f"{B}d"] += 1

                    # bookmaker dispersion (how many books carry this market)
                    books = e.get("bookmakers", []) or []
                    dispersions.append(len(books))

                    # opening/closing presence (approx: presence of "last_update" far from commence vs near)
                    # We treat presence if any bookmaker has a last_update timestamp.
                    has_open, has_close = False, False
                    for b in books:
                        for mk in b.get("markets", []) or []:
                            if mk.get("key") in ("h2h","totals","btts","spreads"):
                                last_up = mk.get("last_update")
                                if last_up:
                                    # opening = updated > 48h before KO; closing = updated < 3h before KO
                                    try:
                                        lu = datetime.fromisoformat(last_up.replace("Z","+00:00")).astimezone(timezone.utc)
                                        if start:
                                            ko = datetime.fromisoformat(start.replace("Z","+00:00")).astimezone(timezone.utc)
                                            hours_to_ko = (ko - lu).total_seconds()/3600.0
                                            if hours_to_ko >= 48: has_open = True
                                            if hours_to_ko <= 3: has_close = True
                                    except Exception:
                                        pass
                    league_stats["has_opening_odds"] += 1 if has_open else 0
                    league_stats["has_closing_odds"] += 1 if has_close else 0

                if dispersions:
                    s = pd.Series(dispersions)
                    league_stats["bookmaker_dispersion_avg"] = round(float(s.mean()), 3)
                    league_stats["bookmaker_dispersion_median"] = float(s.median())

                if events:
                    league_stats["sample_event"] = events[0]
            else:
                league_stats["errors"] = f"HTTP {getattr(ev,'status_code',None)}"

        except Exception as e:
            league_stats["errors"] = str(e)

        meta["leagues"][lg] = league_stats

    # Aggregate future odds counts across leagues
    for lg, st in meta["leagues"].items():
        for k, v in st["by_bucket"].items():
            meta["future_odds_counts"][k] += v

    write_json(meta, args.out)

if __name__ == "__main__":
    main()