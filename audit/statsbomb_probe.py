import argparse
from utils import write_json

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--token", default="")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    # For open data, StatsBomb is limited in leagues/years; paid expands.
    caps = {
        "ok": True,
        "token_present": bool(args.token),
        "fields_available": ["shot_events","freeze_frames","keeper_actions","pressures"],
        "note": "Probe your licensed endpoints if you have a token; otherwise mark open-data limits."
    }
    write_json(caps, args.out)

if __name__ == "__main__":
    main()