import argparse
from utils import write_json

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    # Understat probing depends on your current client; we record capability intent
    caps = {
        "ok": True,  # set to False if your client fails
        "fields_available": ["match_id","date","home_team","away_team","xg_home","xg_away","league","players_xg"],
        "note": "Implement your Understat client here; this probe just records intended schema."
    }
    write_json(caps, args.out)

if __name__ == "__main__":
    main()