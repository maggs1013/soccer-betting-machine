import argparse, os
from utils import write_json, file_age_days

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    # If you maintain a local understat cache, detect staleness here
    cache_path = "data/xg_understat.csv"
    caps = {
        "ok": True,  # flip to False if your client fetch fails
        "fields_available": ["match_id","date","home_team","away_team","xg_home","xg_away","league","players_xg"],
        "note": "Integrate your Understat client here and update 'ok' accordingly.",
        "cache_path": cache_path,
        "cache_stale_days": file_age_days(cache_path)
    }
    write_json(caps, args.out)

if __name__ == "__main__":
    main()