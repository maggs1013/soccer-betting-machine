import argparse, pandas as pd
from util_io import read_csv_safe, write_csv
from util_textnorm import alias_canonical, normalize_team_name

parser = argparse.ArgumentParser()
parser.add_argument("--fixtures", required=True)
parser.add_argument("--out", required=True)
parser.add_argument("--merge-mode", default="soft")
args = parser.parse_args()

fx = read_csv_safe(args.fixtures)
if fx.empty:
    raise SystemExit("Fixtures file is empty.")

teams = pd.Series(pd.concat([fx["home_team"], fx["away_team"]])).dropna().unique()
df = pd.DataFrame({
    "source_name": teams,
    "canonical_team": [alias_canonical(t) for t in teams],
    "norm": [normalize_team_name(t) for t in teams],
    "league": fx.get("league", pd.Series(["unknown"]*len(teams))).iloc[:len(teams)].values
})

# de-duplicate canonical
df = df.sort_values("canonical_team").drop_duplicates("canonical_team")
write_csv(df[["source_name","canonical_team","norm","league"]], args.out)
print(f"Wrote {len(df)} rows to {args.out}")