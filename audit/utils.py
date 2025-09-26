import json, time, pathlib, requests
from datetime import datetime, timedelta
from typing import Tuple

def http_get(url, headers=None, params=None, timeout=20, tries=3, sleep=1.5):
    last = None
    for _ in range(tries):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout)
            last = r
            if r.status_code < 500:
                return r
        except requests.RequestException as e:
            last = e
        time.sleep(sleep)
    return last

def write_json(obj, path):
    pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)

def horizon_dates(days=7):
    now = datetime.utcnow().date()
    return str(now), str(now + timedelta(days=days))