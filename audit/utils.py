import json, time, pathlib, requests, os
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

def http_get(url, headers=None, params=None, timeout=20, tries=3, sleep=1.5):
    last = None
    for _ in range(tries):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout)
            last = r
            # return on any non-5xx (rate limit 429 will be returned for caller to handle)
            if r.status_code < 500:
                return r
        except requests.RequestException as e:
            last = e
        time.sleep(sleep)
    return last

def write_json(obj: Dict[str, Any], path: str):
    pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)

def horizon_dates(days=7):
    now = datetime.utcnow()
    return now.date(), (now + timedelta(days=days)).date()

def file_age_days(path: str) -> Optional[float]:
    try:
        mtime = os.path.getmtime(path)
        age = (time.time() - mtime) / (3600*24.0)
        return round(age, 3)
    except Exception:
        return None

def ensure_dir(path: str):
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)