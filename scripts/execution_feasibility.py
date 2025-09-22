#!/usr/bin/env python3
import os
import pandas as pd
from datetime import datetime

RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
os.makedirs(RUN_DIR, exist_ok=True)

ODDS_SNAPSHOT = os.path.join("data", "odds_upcoming.csv")  # unified odds table
OUT = os.path.join(RUN_DIR, "EXECUTION_FEASIBILITY.csv")

def main():
    df = pd.read_csv(ODDS_SNAPSHOT)
    # Heuristics: coverage by number of books seen, spread width proxies
    if "num_books" not in df.columns:
        df["num_books"] = 1
    df["feasible"] = (df["num_books"] >= 2).astype(int)
    df["note"] = df["num_books"].map(lambda n: "ok" if n>=2 else "thin")
    out = df[["fixture_id","league","num_books","feasible","note"]].drop_duplicates("fixture_id")
    out.to_csv(OUT, index=False)
    print("EXECUTION_FEASIBILITY.csv written:", len(out))

if __name__ == "__main__":
    main()