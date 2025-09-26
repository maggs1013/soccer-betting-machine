#!/usr/bin/env python3
"""
model_matrix_build.py — produce a clean, model-ready matrix

Inputs:
  data/UPCOMING_7D_enriched.csv   (from enrich_features.py)
  data/UPCOMING_7D_features.csv   (from feature_patcher.py)

Outputs:
  data/UPCOMING_7D_model_matrix.csv   (numeric-ready, with stable IDs)
  reports/MODEL_MATRIX_SCHEMA.md      (column list + basic stats)

Behavior:
- merges on fixture_id
- preserves key identifiers (fixture_id, date, league, home_team, away_team)
- selects numeric columns for training (float/int), leaves IDs separately
- safe: if an input is missing/empty, writes header-only outputs
"""

import os
import numpy as np
import pandas as pd

DATA = "data"
REPORTS = "reports"
ENR = os.path.join(DATA, "UPCOMING_7D_enriched.csv")
FEA = os.path.join(DATA, "UPCOMING_7D_features.csv")
OUT = os.path.join(DATA, "UPCOMING_7D_model_matrix.csv")
SCHEMA = os.path.join(REPORTS, "MODEL_MATRIX_SCHEMA.md")

ID_COLS = ["fixture_id", "date", "league", "home_team", "away_team"]

def safe_read(path):
    if not os.path.exists(path): return pd.DataFrame()
    try: return pd.read_csv(path)
    except Exception: return pd.DataFrame()

def ensure_fixture_id(df):
    if "fixture_id" in df.columns:
        return df
    # last-resort fixture_id
    def mk(r):
        d = str(r.get("date","NA")).replace("-","").replace("T","_").replace(":","")
        h = str(r.get("home_team","NA")).strip().lower().replace(" ","_")
        a = str(r.get("away_team","NA")).strip().lower().replace(" ","_")
        return f"{d}__{h}__vs__{a}"
    df["fixture_id"] = df.apply(mk, axis=1)
    return df

def main():
    os.makedirs(DATA, exist_ok=True)
    os.makedirs(REPORTS, exist_ok=True)

    enr = safe_read(ENR)
    fea = safe_read(FEA)

    if enr.empty and fea.empty:
        pd.DataFrame(columns=ID_COLS).to_csv(OUT, index=False)
        with open(SCHEMA, "w", encoding="utf-8") as f:
            f.write("# MODEL MATRIX SCHEMA\n\n- No inputs found.\n")
        print("model_matrix_build: no input tables; wrote header-only")
        return

    if enr.empty:
        # build minimal shell from features
        base = fea.copy()
    elif fea.empty:
        base = enr.copy()
    else:
        # full merge
        enr = ensure_fixture_id(enr)
        fea = ensure_fixture_id(fea)
        base = pd.merge(enr, fea, on="fixture_id", how="left", suffixes=("", "_fea"))

    # keep IDs (create if missing)
    for c in ID_COLS:
        if c not in base.columns:
            base[c] = np.nan

    # move IDs to front
    id_part = base[ID_COLS].copy()
    # numeric feature selection (exclude obvious IDs/text)
    numeric_cols = []
    for c in base.columns:
        if c in ID_COLS: 
            continue
        dt = str(base[c].dtype)
        if any(k in dt for k in ("float","int")):
            numeric_cols.append(c)

    X = base[numeric_cols].copy() if numeric_cols else pd.DataFrame()
    # Optional: simple NaN handling (leave NaN; models/calibration can handle; or fillna(0) if desired)
    # X = X.fillna(0)

    out = pd.concat([id_part, X], axis=1)
    out.to_csv(OUT, index=False)

    # schema dump
    with open(SCHEMA, "w", encoding="utf-8") as f:
        f.write("# MODEL MATRIX SCHEMA\n\n")
        f.write(f"- Rows: {len(out)}\n")
        f.write(f"- Numeric feature count: {len(numeric_cols)}\n\n")
        f.write("## ID Columns\n")
        for c in ID_COLS: f.write(f"- {c}\n")
        f.write("\n## Numeric Features\n")
        if numeric_cols:
            for c in numeric_cols: f.write(f"- {c}\n")
        else:
            f.write("- (none detected)\n")
    print(f"model_matrix_build: wrote {OUT} with {len(numeric_cols)} numeric cols")

if __name__ == "__main__":
    main()