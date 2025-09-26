import argparse, pandas as pd, time
from utils import write_json

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", default=".cache/soccerdata")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    info = {"fbref": {}, "errors": []}
    t0 = time.time()
    try:
        # Lazy import to avoid installing if not needed elsewhere
        import soccerdata as sd
        # Example: probe EPL team stats availability (schema + rows)
        fb = sd.FBref(cache=args.cache)
        df = fb.read_team_season_stats(competition="ENG-Premier League", season="2024-2025")
        info["fbref"]["ok"] = True
        info["fbref"]["rows"] = len(df)
        info["fbref"]["cols"] = list(df.columns)
    except Exception as e:
        info["fbref"]["ok"] = False
        info["errors"].append(str(e))

    info["ms"] = round((time.time() - t0) * 1000, 1)
    write_json(info, args.out)

if __name__ == "__main__":
    main()