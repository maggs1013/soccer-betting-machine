import argparse, requests
from utils import write_json

INDEX = "https://www.football-data.co.uk/englandm.php"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    meta = {"ok": False, "datasets": [], "fields_available": [], "note": ""}

    try:
        r = requests.get(INDEX, timeout=25)
        meta["ok"] = (r.status_code == 200)
        meta["note"] = "Index reachable; maintain curated CSV list per league/season for reliability."
    except Exception as e:
        meta["note"] = f"Probe failed: {e}"

    write_json(meta, args.out)

if __name__ == "__main__":
    main()