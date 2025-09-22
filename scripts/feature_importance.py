# scripts/feature_importance.py
# Export coefficients (absolute importance) for the logistic feature model.
# Reads: data/feature_model.pkl, data/feature_model_features.json
# Writes: data/FEATURE_IMPORTANCE.csv

import os, json, pickle, pandas as pd, numpy as np

DATA="data"
MOD = os.path.join(DATA,"feature_model.pkl")
JSN = os.path.join(DATA,"feature_model_features.json")
OUT = os.path.join(DATA,"FEATURE_IMPORTANCE.csv")

def main():
    if not (os.path.exists(MOD) and os.path.exists(JSN)):
        pd.DataFrame(columns=["feature","coefficient","abs_coeff","rank"]).to_csv(OUT,index=False)
        print(f"[WARN] Missing model files; wrote empty {OUT}")
        return
    pack = pickle.load(open(MOD,"rb"))
    feat_names = json.load(open(JSN,"r")).get("feat_names",[])
    model = pack.get("model", None)
    if model is None or not hasattr(model,"coef_") or not feat_names:
        pd.DataFrame(columns=["feature","coefficient","abs_coeff","rank"]).to_csv(OUT,index=False)
        print(f"[WARN] Model has no coefficients; wrote empty {OUT}")
        return
    # Multinomial: coef_ shape (3, n_features). Use L2 norm across classes.
    coefs = np.linalg.norm(model.coef_, axis=0)
    df = pd.DataFrame({"feature": feat_names, "coefficient": coefs, "abs_coeff": np.abs(coefs)})
    df["rank"] = df["abs_coeff"].rank(ascending=False, method="dense").astype(int)
    df.sort_values("abs_coeff", ascending=False, inplace=True)
    df.to_csv(OUT,index=False)
    print(f"[OK] wrote {OUT}")

if __name__ == "__main__":
    main()