import argparse
from utils import write_json, file_age_days

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--token", default="")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    # You can expand here to hit your licensed endpoints conditionally
    caps = {
        "ok": True,
        "token_present": bool(args.token),
        "fields_available": ["shot_events","freeze_frames","keeper_actions","pressures"],
        "note": "Probe your licensed endpoints if token present; otherwise mark open-data limits.",
        "cache_path": "data/xg_statsbomb.csv",
        "cache_stale_days": file_age_days("data/xg_statsbomb.csv")
    }
    write_json(caps, args.out)

if __name__ == "__main__":
    main()