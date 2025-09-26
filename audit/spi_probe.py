import argparse, pandas as pd, io, requests, os
from utils import write_json, file_age_days

SPI_URL = "https://projects.fivethirtyeight.com/soccer-api/club/spi_global_rankings2.csv"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", default="data")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    out = {
        "ok": False, "rows": 0, "cols": [], "fields_available": [],
        "sample": None, "cache_path": None, "cache_stale_days": None, "error": None
    }

    cache_path = os.path.join(args.cache, "sd_538_spi.csv")
    out["cache_path"] = cache_path

    # Try remote
    try:
        r = requests.get(SPI_URL, timeout=25)
        if r.status_code == 200 and r.text:
            df = pd.read_csv(io.StringIO(r.text))
            out["ok"] = True
            out["rows"] = len(df)
            out["cols"] = list(df.columns)
            out["fields_available"] = list(df.columns)
            out["sample"] = df.head(5).to_dict(orient="records")
            # store snapshot for cache usage by main pipeline if desired
            try:
                df.to_csv(cache_path, index=False)
            except Exception:
                pass
        else:
            out["error"] = f"HTTP {r.status_code}"
    except Exception as e:
        out["error"] = str(e)

    # If remote failed, try local cache
    if not out["ok"] and os.path.exists(cache_path):
        try:
            df = pd.read_csv(cache_path)
            out["ok"] = True
            out["rows"] = len(df)
            out["cols"] = list(df.columns)
            out["fields_available"] = list(df.columns)
            out["sample"] = df.head(5).to_dict(orient="records")
        except Exception as e:
            out["error"] = f"cache load failed: {e}"

    # Record staleness of cache (if present)
    out["cache_stale_days"] = file_age_days(cache_path)

    write_json(out, args.out)

if __name__ == "__main__":
    main()