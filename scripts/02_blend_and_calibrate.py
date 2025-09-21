import argparse, json, numpy as np, pandas as pd
from util_io import read_csv_safe, write_csv
from math import exp

def poisson_p(x, lam):
    # P(X=x) for Poisson(λ)
    return lam**x * exp(-lam) / np.math.factorial(x)

def match_probs_poisson(lh, la, maxg=8):
    pm = np.zeros((maxg+1, maxg+1))
    for i in range(maxg+1):
        for j in range(maxg+1):
            pm[i,j] = poisson_p(i, lh) * poisson_p(j, la)
    ph = float(np.tril(pm, -1).sum())
    pd = float(np.trace(pm))
    pa = float(np.triu(pm, +1).sum())
    return ph, pd, pa

parser = argparse.ArgumentParser()
parser.add_argument("--enriched", required=True)
parser.add_argument("--blend", required=True)
parser.add_argument("--calibrator", required=True)
parser.add_argument("--out", required=True)
args = parser.parse_args()

en = read_csv_safe(args.enriched)
with open(args.blend,"r") as f:
    blend = json.load(f)
w_market = float(blend.get("w_market", 0.85))

# market probs columns (if present)
mcols = [("home_prob","draw_prob","away_prob"), ("odds_home_prob","odds_draw_prob","odds_away_prob")]
def pick_market_cols(df):
    for a,b,c in mcols:
        if a in df.columns and b in df.columns and c in df.columns:
            return a,b,c
    return None

market_cols = pick_market_cols(en)

out = en.copy()

# model-only probs via Poisson
mh, md, ma = [], [], []
for _,r in out.iterrows():
    ph, pd, pa = match_probs_poisson(float(r["home_xg"]), float(r["away_xg"]))
    mh.append(ph); md.append(pd); ma.append(pa)
out["model_ph"], out["model_pd"], out["model_pa"] = mh, md, ma

# blended probs
if market_cols:
    a,b,c = market_cols
    out["blend_ph"] = w_market*out[a] + (1-w_market)*out["model_ph"]
    out["blend_pd"] = w_market*out[b] + (1-w_market)*out["model_pd"]
    out["blend_pa"] = w_market*out[c] + (1-w_market)*out["model_pa"]
else:
    out["blend_ph"] = out["model_ph"]
    out["blend_pd"] = out["model_pd"]
    out["blend_pa"] = out["model_pa"]

# normalize to sum=1
s = out[["blend_ph","blend_pd","blend_pa"]].sum(axis=1).replace(0,1)
out["ph"] = out["blend_ph"]/s
out["pd"] = out["blend_pd"]/s
out["pa"] = out["blend_pa"]/s

# attempt calibration
try:
    import pickle
    with open(args.calibrator, "rb") as f:
        calib = pickle.load(f)  # should accept array-like, return calibrated probs or scaling
    P = np.vstack([out["ph"].values, out["pd"].values, out["pa"].values]).T
    Pc = calib.transform(P) if hasattr(calib, "transform") else calib(P)
    out["ph_c"], out["pd_c"], out["pa_c"] = Pc[:,0], Pc[:,1], Pc[:,2]
    out["ph"], out["pd"], out["pa"] = out["ph_c"], out["pd_c"], out["pa_c"]
except Exception as e:
    # keep uncalibrated if calibrator not usable here
    pass

# Top pick and EV
out["top_pick"] = out[["ph","pd","pa"]].idxmax(axis=1).map({"ph":"H","pd":"D","pa":"A"})
out["confidence"] = out[["ph","pd","pa"]].max(axis=1)

write_csv(out[[
    "home_team","away_team","date" if "date" in out.columns else out.columns[0],
    "home_xg","away_xg",
    "ph","pd","pa","top_pick","confidence"
]].rename(columns={out.columns[0]:"date"}), args.out)
print(f"Wrote predictions to {args.out} with {len(out)} rows.")