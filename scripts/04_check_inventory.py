import argparse, pandas as pd, os

parser = argparse.ArgumentParser()
parser.add_argument("--report", required=True)
parser.add_argument("--out", required=True)
args = parser.parse_args()

lines = []
def ok(p):
    return os.path.exists(p) and os.path.getsize(p) > 0

paths = [
    "data/UPCOMING_fixtures.csv",
    "data/teams_master.csv",
    "data/LEAGUE_XG_TABLE.csv",
    "data/xg_statsbomb.csv",
    "data/team_statsbomb_features.csv",
    "data/xg_understat.csv",
    "data/team_form_features.csv",
    "data/sd_538_spi.csv",
    "data/sd_fbref_team_stats.csv",
    "data/manual_odds.csv",
    "outputs/UPCOMING_7D_enriched.csv",
    "outputs/PREDICTIONS_7D.csv",
    "outputs/DATA_QUALITY_REPORT.csv",
]

for p in paths:
    lines.append(f"[{'OK' if ok(p) else 'MISS'}] {p}")

with open(args.out,"w") as f:
    f.write("\n".join(lines))
print(f"Wrote inventory log to {args.out}")