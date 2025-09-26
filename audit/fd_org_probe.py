import argparse, pandas as pd, io, requests, re
from utils import write_json

# Football-Data.org season CSV index: static file list page; adjust if you keep a curated list
INDEX = "https://www.football-data.co.uk/englandm.php"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    meta = {"ok": False, "datasets": [], "fields_available": [], "note": ""}

    try:
        r = requests.get(INDEX, timeout=25)
        meta["ok"] = (r.status_code == 200)
        # We don’t scrape aggressively here; you may maintain a curated list instead
        meta["note"] = "Index reachable; recommend curated CSV list per league/season."
    except Exception as e:
        meta["note"] = f"Probe failed: {e}"

    write_json(meta, args.out)

if __name__ == "__main__":
    main()