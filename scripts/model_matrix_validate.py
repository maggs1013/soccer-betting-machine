#!/usr/bin/env python3
"""
model_matrix_validate.py — ensure minimum viable schema exists
Inputs: data/UPCOMING_7D_model_matrix.csv
Outputs: reports/MODEL_MATRIX_SCHEMA.md (appends validation block)
Fail policy: does NOT fail the run; writes warnings so Council can act.
"""

import os, pandas as pd

DATA="data"
REPORTS="reports"
MATRIX=os.path.join(DATA,"UPCOMING_7D_model_matrix.csv")
SCHEMA=os.path.join(REPORTS,"MODEL_MATRIX_SCHEMA.md")

REQUIRED_ID = ["fixture_id","date","league","home_team","away_team"]
RECOMMENDED_FEATURES = [
    "spi_rank_diff","injury_index_diff","availability_diff",
    "market_dispersion","ou_main_total","btts_price_gap"
]

def safe_read(p):
    if not os.path.exists(p): return pd.DataFrame()
    try: return pd.read_csv(p)
    except Exception: return pd.DataFrame()

def main():
    os.makedirs(REPORTS, exist_ok=True)
    mm = safe_read(MATRIX)
    lines = []
    lines.append("\n---\n## Validation Results\n")
    if mm.empty:
        lines.append("- ❌ Model matrix is empty.")
    else:
        # check IDs
        missing_id = [c for c in REQUIRED_ID if c not in mm.columns]
        if missing_id:
            lines.append(f"- ❌ Missing ID columns: {', '.join(missing_id)}")
        else:
            lines.append("- ✅ All required ID columns present.")

        # check recommended features
        missing_feat = [c for c in RECOMMENDED_FEATURES if c not in mm.columns]
        if missing_feat:
            lines.append(f"- ⚠️ Missing recommended features: {', '.join(missing_feat)}")
        else:
            lines.append("- ✅ Recommended feature set present.")

        # simple sanity: numeric feature count
        num_count = sum(str(mm[c].dtype).startswith(("float","int")) for c in mm.columns)
        lines.append(f"- ℹ️ Numeric columns detected: {num_count}")

    # append to schema file (create if needed)
    if not os.path.exists(SCHEMA):
        with open(SCHEMA,"w",encoding="utf-8") as f: f.write("# MODEL MATRIX SCHEMA\n")
    with open(SCHEMA,"a",encoding="utf-8") as f: f.write("\n".join(lines))
    print("model_matrix_validate: appended validation to schema")

if __name__ == "__main__":
    main()