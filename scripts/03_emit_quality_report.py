import argparse, pandas as pd, numpy as np
from util_io import read_csv_safe, write_csv

parser = argparse.ArgumentParser()
parser.add_argument("--enriched", required=True)
parser.add_argument("--pred", required=True)
parser.add_argument("--out", required=True)
args = parser.parse_args()

en = read_csv_safe(args.enriched)
pr = read_csv_safe(args.pred)

rep = pd.DataFrame({
    "fixtures_rows":[len(en)],
    "pred_rows":[len(pr)],
    "home_xg_nan":[int(en["home_xg"].isna().sum()) if "home_xg" in en.columns else len(en)],
    "away_xg_nan":[int(en["away_xg"].isna().sum()) if "away_xg" in en.columns else len(en)],
    "feature_fill_mean":[float(en.get("feature_fill_ratio", pd.Series([0]*len(en))).mean()) if len(en) else 0.0],
    "probs_sum_ok":[int((abs(pr[["ph","pd","pa"]].sum(axis=1)-1.0) < 1e-6).mean()*100) if not pr.empty else 0],
})

write_csv(rep, args.out)
print(f"Wrote quality report to {args.out}")