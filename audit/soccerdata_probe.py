import argparse, time, os
from utils import write_json, file_age_days

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", default=".cache/soccerdata")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    info = {
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "fbref": {"ok": False, "rows": 0, "cols": [], "cache_dir": args.cache, "cache_stale_days": None},
        "spi_via_soccerdata": {"ok": False, "rows": 0, "cols": []},
        "errors": []
    }

    t0 = time.time()
    try:
        import soccerdata as sd
        fb = sd.FBref(cache=args.cache)
        # Probe any popular league season to validate schema — adjust as you like
        df = fb.read_team_season_stats(competition="ENG-Premier League", season="2024-2025")
        info["fbref"]["ok"] = True
        info["fbref"]["rows"] = len(df)
        info["fbref"]["cols"] = list(df.columns)
    except Exception as e:
        info["fbref"]["ok"] = False
        info["errors"].append(f"FBref: {e}")

    # Estimate cache staleness (mtime of cache dir)
    try:
        ages = []
        for root, _, files in os.walk(args.cache):
            for f in files:
                p = os.path.join(root, f)
                age = file_age_days(p)
                if age is not None:
                    ages.append(age)
        if ages:
            info["fbref"]["cache_stale_days"] = round(min(ages), 3)  # since newest write
    except Exception:
        pass

    info["ms"] = round((time.time() - t0) * 1000, 1)
    write_json(info, args.out)

if __name__ == "__main__":
    main()