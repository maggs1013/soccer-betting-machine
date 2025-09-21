import argparse, pandas as pd, numpy as np
from util_io import read_csv_safe, write_csv

ap = argparse.ArgumentParser()
ap.add_argument("--enriched", required=True)
ap.add_argument("--pred", required=True)
ap.add_argument("--out", required=True)
args = ap.parse_args()

en = read_csv_safe(args.enriched)
pr = read_csv_safe(args.pred)

rep = {}
rep["fixtures_rows"] = len(en)
rep["pred_rows"] = len(pr)
rep["home_xg_zero"] = int((en["home_xg"]==0).sum()) if "home_xg" in en.columns else -1
rep["away_xg_zero"] = int((en["away_xg"]==0).sum()) if "away_xg" in en.columns else -1
rep["feature_fill_mean"] = float(en.get("feature_fill_ratio", pd.Series([0]*len(en))).mean()) if len(en) else 0.0
rep["probs_sum_ok_pct"] = int((abs(pr[["ph","pd","pa"]].sum(axis=1)-1.0) < 1e-6).mean()*100) if not pr.empty else 0
rep["understat_zero_flag"] = "yes" if ("zero_source_flags" in en.columns and "understat_zero=yes" in en["zero_source_flags"].iloc[0]) else "no"
write_csv(pd.DataFrame([rep]), args.out)
print(f"[quality] wrote → {args.out}")