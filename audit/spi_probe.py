import argparse, pandas as pd, io, requests
from utils import write_json

SPI_CSV = "https://projects.fivethirtyeight.com/soccer-api/club/spi_global_rankings2.csv"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    meta = {"ok": False, "rows": 0, "cols": [], "fields_available": [], "sample": None}
    try:
        r = requests.get(SPI_CSV, timeout=25)
        if r.status_code == 200 and r.text:
            df = pd.read_csv(io.StringIO(r.text))
            meta["ok"] = True
            meta["rows"] = len(df)
            meta["cols"] = list(df.columns)
            meta["fields_available"] = list(df.columns)
            meta["sample"] = df.head(5).to_dict(orient="records")
    except Exception as e:
        meta["error"] = str(e)
    write_json(meta, args.out)

if __name__ == "__main__":
    main()