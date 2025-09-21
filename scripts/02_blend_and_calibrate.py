import argparse, json, numpy as np, pandas as pd
from util_io import read_csv_safe, write_csv
from math import exp

def poisson_p(x, lam): return lam**x * exp(-lam) / np.math.factorial(x)

def match_probs_poisson(lh, la, maxg=8):
    pm = np.zeros((maxg+1, maxg+1))
    for i in range(maxg+1):
        for j in range(maxg+1):
            pm[i,j] = poisson_p(i, lh) * poisson_p(j, la)
    ph = float(np.tril(pm, -1).sum())
    pd = float(np.trace(pm))
    pa = float(np.triu(pm, +1).sum())
    return ph, pd, pa

ap = argparse.ArgumentParser()
ap.add_argument("--enriched", required=True)
ap.add_argument("--blend", required=True)
ap.add_argument("--calibrator", required=True)
ap.add_argument("--out", required=True)
args = ap.parse_args()

en = read_csv_safe(args.enriched)
cfg = json.load(open(args.blend))
base_wm = float(cfg.get("w_market", 0.85))

# Market columns (if upstream provided)
cand = [("home_prob","draw_prob","away_prob"), ("odds_home_prob","odds_draw_prob","odds_away_prob")]
mcols = next((c for c in cand if all(x in en.columns for x in c)), None)

rows = []
for _,r in en.iterrows():
    ph_m, pd_m, pa_m = None, None, None
    if mcols:
        ph_m, pd_m, pa_m = float(r[mcols[0]]), float(r[mcols[1]]), float(r[mcols[2]])

    ph_model, pd_model, pa_model = match_probs_poisson(float(r["home_xg"]), float(r["away_xg"]))
    # adaptive weight: if features are thin, lean on market; else lean on model more
    fill = float(r.get("feature_fill_ratio", 1.0))
    w_market = base_wm + (1 - fill)*0.10   # add up to +0.10 if features thin
    w_market = max(0.0, min(1.0, w_market))

    if ph_m is None:
        ph, pd, pa = ph_model, pd_model, pa_model
    else:
        ph = w_market*ph_m + (1-w_market)*ph_model
        pd = w_market*pd_m + (1-w_market)*pd_model
        pa = w_market*pa_m + (1-w_market)*pa_model

    s = ph+pd+pa or 1.0
    rows.append((ph/s, pd/s, pa/s, w_market))

out = en.copy()
out[["ph","pd","pa","w_market_used"]] = pd.DataFrame(rows, index=en.index)

# Calibrate if possible
try:
    import pickle
    cal = pickle.load(open(args.calibrator,"rb"))
    P = np.vstack([out["ph"].values, out["pd"].values, out["pa"].values]).T
    Pc = cal.transform(P) if hasattr(cal, "transform") else cal(P)
    out["ph"], out["pd"], out["pa"] = Pc[:,0], Pc[:,1], Pc[:,2]
except Exception:
    pass

out["top_pick"] = out[["ph","pd","pa"]].idxmax(axis=1).map({"ph":"H","pd":"D","pa":"A"})
out["confidence"] = out[["ph","pd","pa"]].max(axis=1)

write_csv(out[[
    "home_team","away_team","date" if "date" in out.columns else out.columns[0],
    "home_xg","away_xg","ph","pd","pa","top_pick","confidence","w_market_used"
]].rename(columns={out.columns[0]:"date"}), args.out)
print(f"[blend] wrote {len(out)} rows → {args.out}")